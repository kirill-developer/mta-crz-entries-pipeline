from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from google.cloud import bigquery
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

def load_gcs_to_bigquery(full_refresh: bool = False):
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

        # Initialize BigQuery client
        client = bigquery.Client(
            credentials=credentials,
            project=creds_data['project_id']
        )

        # Configure the load job
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE if full_refresh
                              else bigquery.WriteDisposition.WRITE_APPEND,
        )

        # Construct the full table ID
        table_id = f"{creds_data['project_id']}.{DATASET_NAME}.{TABLE_NAME}"

        # Determine which GCS path to use
        gcs_uri = f"{GCS_BUCKET}/{GCS_PATH}" if full_refresh else f"{GCS_BUCKET}/{GCS_DAILY_PATH}"

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
        schedule_interval='0 3 * * *',  # Run at 3 AM daily, after the daily update DAG
        catchup=False,
        tags=['nyc_traffic'],
) as daily_dag:
    daily_load_task = PythonOperator(
        task_id='daily_load_to_bigquery',
        python_callable=load_gcs_to_bigquery,
        op_kwargs={'full_refresh': False},
        execution_timeout=timedelta(hours=2),
    )