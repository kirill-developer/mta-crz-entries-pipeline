from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from dlt.common import json
import dlt
import requests
from airflow.models import Variable
from airflow.exceptions import AirflowException
import os
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pathlib import Path

# Constants for Congestion Pricing dataset
CHECKPOINT_DIR = "/opt/airflow/data/checkpoints"
CHECKPOINT_FILE = f"{CHECKPOINT_DIR}/nyc_crz_entries_daily_checkpoint.json"
API_BASE_URL = "https://data.ny.gov/api/odata/v4/t6yz-b64h"
SELECT_FIELDS = "toll_date,toll_hour,toll_10_minute_block,minute_of_hour,hour_of_day,day_of_week_int,day_of_week,toll_week,time_period,vehicle_class,detection_group,detection_region,crz_entries,excluded_roadway_entries"
DAYS_TO_FETCH = 1  # Fetch data for the last day

# Ensure checkpoint directory exists
Path(CHECKPOINT_DIR).mkdir(parents=True, exist_ok=True)


class NYCRZEntriesExtractor:
    def __init__(self):
        self.session = requests.Session()
        adapter = HTTPAdapter(
            max_retries=Retry(
                total=3,
                backoff_factor=1,
                status_forcelist=[408, 429, 500, 502, 503, 504],
                allowed_methods=["GET"]
            )
        )
        self.session.mount("https://", adapter)
        self.session.timeout = 300  # 5 minutes per request

    def fetch_batch(self, url: str) -> tuple:
        """Fetch a batch of records with error handling"""
        try:
            print(f"Fetching URL: {url}")
            response = self.session.get(url)
            response.raise_for_status()
            data = response.json()
            return data.get("value", []), data.get("@odata.nextLink")
        except requests.exceptions.RequestException as e:
            print(f"API request failed: {str(e)}")
            if hasattr(e, 'response') and e.response:
                print(f"Response content: {e.response.text}")
            raise AirflowException(f"API request failed: {str(e)}")


def ensure_dlt_config() -> bool:
    """Safe configuration setup with validation"""
    try:
        bucket_url = Variable.get(
            "DLT_FILESYSTEM_BUCKET_URL",
            default_var="gs://terraform-nyctrafficanalysis-bucket"
        )
        if not bucket_url.startswith("gs://"):
            raise ValueError("Invalid GCS bucket URL format")
        os.environ['DESTINATION__FILESYSTEM__BUCKET_URL'] = bucket_url
        return True
    except Exception as e:
        raise AirflowException(f"Configuration failed: {str(e)}")


def load_daily_updates():
    if not ensure_dlt_config():
        raise AirflowException("Configuration failed")

    with open("/opt/airflow/keys/my-creds.json") as f:
        creds = json.load(f)

    @dlt.resource(
        table_name="nyc_crz_entries_daily",
        write_disposition="append",
    )
    def crz_entries_daily_data():
        extractor = NYCRZEntriesExtractor()

        # Calculate date range for daily updates
        end_date = datetime.utcnow().date()
        start_date = end_date - timedelta(days=DAYS_TO_FETCH)

        # Build URL with date filter
        base_url = (
            f"{API_BASE_URL}?"
            f"$select={SELECT_FIELDS}&"
            f"$filter=toll_date ge {start_date.isoformat()} and toll_date le {end_date.isoformat()}&"
            f"$orderby=toll_date,toll_hour"
        )

        url = base_url
        print(f"Fetching daily updates for date range: {start_date} to {end_date}")

        try:
            while url:
                batch, next_url = extractor.fetch_batch(url)
                if not batch:
                    print("No new records found for today")
                    break

                print(f"Fetched {len(batch)} records for daily update")
                yield batch

                url = next_url
                if url:
                    time.sleep(1)  # Rate limiting

        except Exception as e:
            print(f"Daily extraction failed: {str(e)}")
            raise

    try:
        pipeline = dlt.pipeline(
            pipeline_name="nyc_crz_entries_daily",
            destination="filesystem",
            dataset_name="nyc_crz_data_daily",
        )

        load_info = pipeline.run(
            crz_entries_daily_data(),
            loader_file_format="parquet",
            credentials={
                "project_id": creds["project_id"],
                "private_key": creds["private_key"],
                "client_email": creds["client_email"],
                "token_uri": creds["token_uri"],
            }
        )

        print(f"Load completed!")
        return load_info

    except Exception as e:
        print(f"Daily pipeline failed: {str(e)}")
        raise AirflowException(f"Daily pipeline failed: {str(e)}")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=15),
    'max_active_runs': 1,
}

with DAG(
        'nyc_crz_entries_daily_update',
        default_args=default_args,
        schedule_interval='0 2 * * *',  # Run daily at 2 AM
        catchup=False,
        tags=['nyc_traffic'],
) as dag:
    load_task = PythonOperator(
        task_id='load_daily_updates',
        python_callable=load_daily_updates,
        execution_timeout=timedelta(hours=2),
    )