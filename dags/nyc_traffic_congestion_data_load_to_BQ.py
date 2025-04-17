"""
Daily BigQuery load DAG for NYC Traffic Congestion Data.

This DAG loads daily congestion pricing data from GCS to BigQuery.
It follows the same data pipeline pattern as the daily update DAG, processing data from the previous day to ensure:
1. Data completeness - we want to ensure we have complete data for a day before processing it
2. Data latency - there might be a delay in data availability from the source
3. Time zone considerations - different time zones might affect when data is considered "complete" for a day

The DAG runs at 7:10 UTC (3:10 AM ET) daily, 10 minutes after the daily update DAG.
For example, when running on April 16th, it will attempt to load data for April 14th:
- execution_date = April 15th (the day before the DAG runs)
- target_date = April 14th (one day before execution_date)

This ensures we have complete data for the day we're processing and gives the daily update DAG time to complete.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from google.cloud import bigquery, storage
from google.oauth2 import service_account
import json
import os

# Constants
GCS_BUCKET = Variable.get("DLT_FILESYSTEM_BUCKET_URL",
                         default_var="gs://terraform-nyctrafficanalysis-bucket")
GCS_PATH = "nyc_crz_data/nyc_crz_entries"  # Path to full load data directory
GCS_DAILY_PATH = "nyc_crz_data_daily/year_2025_month_04_day_*_nyc_crz_entries_*"  # Path to daily updates
DATASET_NAME = "nyc_traffic"
TABLE_NAME = "congestion_pricing_entries"
CREDENTIALS_PATH = "/opt/airflow/keys/my-creds.json"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=15),
    'max_active_runs': 1,
}

def get_latest_date_in_bigquery(client):
    """Get the latest date for which we have data in BigQuery"""
    try:
        # Query to get the latest date
        query = f"""
        SELECT MAX(toll_date) as latest_date
        FROM `{client.project}.nyc_traffic.fact_traffic`
        """

        query_job = client.query(query)
        results = query_job.result()
        
        for row in results:
            if row.latest_date:
                return row.latest_date.date()
        
        return None
    except Exception as e:
        print(f"Error getting latest date from BigQuery: {str(e)}")
        return None

def load_gcs_to_bigquery(**context):
    """Load data from GCS to BigQuery"""
    try:
        # Load credentials
        if not os.path.exists(CREDENTIALS_PATH):
            raise Exception(f"Credentials file not found at {CREDENTIALS_PATH}")

        with open(CREDENTIALS_PATH) as f:
            creds_data = json.load(f)

        credentials = service_account.Credentials.from_service_account_info(
            creds_data,
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )

        # Initialize clients
        client = bigquery.Client(credentials=credentials, project=creds_data['project_id'])
        storage_client = storage.Client(credentials=credentials, project=creds_data['project_id'])
        bucket = storage_client.bucket(GCS_BUCKET.replace('gs://', ''))

        # Determine which GCS path to use based on whether this is a full refresh
        is_full_refresh = context.get('full_refresh', False)
        if is_full_refresh:
            gcs_uri = f"{GCS_BUCKET}/{GCS_PATH}"
            write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
            print("Performing full refresh load")
            
            blobs = list(bucket.list_blobs(prefix=GCS_PATH))
            if not blobs:
                raise Exception(f"No data files found in {gcs_uri}")
            # Get the most recent file
            latest_blob = max(blobs, key=lambda x: x.time_created)
            gcs_uri = f"{GCS_BUCKET}/{latest_blob.name}"
            print(f"Loading latest file: {latest_blob.name}")
            
            # Configure and execute load job
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition=write_disposition,
            )
            load_job = client.load_table_from_uri(
                gcs_uri,
                f"{creds_data['project_id']}.{DATASET_NAME}.{TABLE_NAME}",
                job_config=job_config
            )
            print(f"Starting BigQuery load job: {load_job.job_id}")
            load_job.result()
            
        else:
            # Get the latest date from BigQuery
            latest_date = get_latest_date_in_bigquery(client)
            print(f"Latest date in BigQuery: {latest_date}")
            
            # List all daily folders
            prefix = "nyc_crz_data_daily/year_2025_month_04_day_"
            blobs = list(bucket.list_blobs(prefix=prefix))
            
            # Extract dates from folder names and sort them
            date_folders = {}
            for blob in blobs:
                # Extract date from folder name using regex
                import re
                match = re.search(r'day_(\d{2})_nyc_crz_entries_(\d{8})', blob.name)
                if match:
                    date_str = match.group(2)
                    date = datetime.strptime(date_str, '%Y%m%d').date()
                    if latest_date is None or date > latest_date:
                        date_folders[date] = blob.name
            
            if not date_folders:
                print("No new daily files found to load")
                return
            
            # Sort dates and process each folder
            for date in sorted(date_folders.keys()):
                blob_name = date_folders[date]
                print(f"Processing data for date: {date}, file: {blob_name}")
                
                gcs_uri = f"{GCS_BUCKET}/{blob_name}"
                
                # Configure and execute load job
                job_config = bigquery.LoadJobConfig(
                    source_format=bigquery.SourceFormat.PARQUET,
                    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                )
                
                load_job = client.load_table_from_uri(
                    gcs_uri,
                    f"{creds_data['project_id']}.{DATASET_NAME}.{TABLE_NAME}",
                    job_config=job_config
                )
                print(f"Starting BigQuery load job for {date}: {load_job.job_id}")
                load_job.result()
                print(f"Successfully loaded data for {date}")

        # Verify the final load
        destination_table = client.get_table(f"{creds_data['project_id']}.{DATASET_NAME}.{TABLE_NAME}")
        print(f"Total rows in table: {destination_table.num_rows}")

    except Exception as e:
        print(f"Failed to load data to BigQuery: {str(e)}")
        raise

# DAG for full refresh
with DAG(
        'nyc_crz_entries_full_load_to_bq',
        default_args=default_args,
        schedule_interval=None,  # Manual trigger for full refresh
        catchup=False,
        tags=['nyc_traffic'],
) as full_refresh_dag:
    full_load_task = PythonOperator(
        task_id='full_load_to_bigquery',
        python_callable=load_gcs_to_bigquery,
        op_kwargs={'full_refresh': True},
        execution_timeout=timedelta(hours=4),
    )

# DAG for daily updates
with DAG(
        'nyc_crz_entries_daily_load_to_bq',
        default_args=default_args,
        schedule_interval='0 8 * * *',  # Run daily at 8:00 AM UTC (4:00 AM ET)
        catchup=False,
        tags=['nyc_traffic'],
) as dag:
    load_task = PythonOperator(
        task_id='daily_load_to_bigquery',
        python_callable=load_gcs_to_bigquery,
        provide_context=True,
        execution_timeout=timedelta(hours=2),
    )