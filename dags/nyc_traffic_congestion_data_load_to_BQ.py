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
GCS_PATH = "nyc_crz_data/nyc_crz_entries/*.parquet"  # Path to full load data
GCS_DAILY_PATH = "nyc_crz_data_daily/nyc_crz_entries_daily/*.parquet"  # Path to daily updates
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

def load_gcs_to_bigquery(**context):
    """Load data from GCS to BigQuery"""
    try:
        # Get the execution date from Airflow context
        execution_date = context.get('execution_date', datetime.now() - timedelta(days=1))
        if isinstance(execution_date, str):
            execution_date = datetime.strptime(execution_date, '%Y-%m-%d').date()
        elif isinstance(execution_date, datetime):
            execution_date = execution_date.date()
        
        # Calculate target date - we want the day before the execution date
        target_date = execution_date - timedelta(days=1)
        print(f"Loading data for date: {target_date}")
        
        # Load credentials
        if not os.path.exists(CREDENTIALS_PATH):
            raise Exception(f"Credentials file not found at {CREDENTIALS_PATH}")

        with open(CREDENTIALS_PATH) as f:
            creds_data = json.load(f)

        credentials = service_account.Credentials.from_service_account_info(
            creds_data,
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )

        # Initialize BigQuery client
        client = bigquery.Client(
            credentials=credentials,
            project=creds_data['project_id']
        )

        # Determine which GCS path to use
        gcs_uri = f"{GCS_BUCKET}/{GCS_DAILY_PATH}"
        
        # Check if there are any files to load
        storage_client = storage.Client(credentials=credentials, project=creds_data['project_id'])
        bucket = storage_client.bucket(GCS_BUCKET.replace('gs://', ''))
        blobs = list(bucket.list_blobs(prefix=GCS_DAILY_PATH))
        
        if not blobs:
            print(f"No new data files found in {gcs_uri}. Skipping load.")
            return

        # Configure the load job
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )

        # Construct the full table ID
        table_id = f"{creds_data['project_id']}.{DATASET_NAME}.{TABLE_NAME}"

        # Execute the load job
        load_job = client.load_table_from_uri(
            gcs_uri,
            table_id,
            job_config=job_config
        )

        print(f"Starting BigQuery load job: {load_job.job_id}")
        load_job.result()  # Wait for job to complete

        # Verify the load
        destination_table = client.get_table(table_id)
        print(f"Successfully loaded {destination_table.num_rows} rows to {table_id}")

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