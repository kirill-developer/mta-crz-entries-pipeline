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
import json
from pathlib import Path
from typing import Optional

# Constants for Congestion Pricing dataset
CHECKPOINT_DIR = "/opt/airflow/data/checkpoints"
CHECKPOINT_FILE = f"{CHECKPOINT_DIR}/nyc_crz_entries_last_skip.json"
BATCH_SIZE = 100000
MAX_RUNTIME = timedelta(hours=12)
API_BASE_URL = "https://data.ny.gov/api/odata/v4/t6yz-b64h"
SELECT_FIELDS = "toll_date,toll_hour,toll_10_minute_block,minute_of_hour,hour_of_day,day_of_week_int,day_of_week,toll_week,time_period,vehicle_class,detection_group,detection_region,crz_entries,excluded_roadway_entries"

# Ensure checkpoint directory exists
Path(CHECKPOINT_DIR).mkdir(parents=True, exist_ok=True)

def save_checkpoint(last_skip: int) -> None:
    """Save progress to a checkpoint file"""
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump({"last_skip": last_skip, "timestamp": datetime.utcnow().isoformat()}, f)

def load_checkpoint() -> Optional[int]:
    """Load progress from checkpoint file"""
    try:
        with open(CHECKPOINT_FILE) as f:
            data = json.load(f)
            return int(data['last_skip'])
    except (FileNotFoundError, json.JSONDecodeError, KeyError):
        return None

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

def load_full_dataset():
    if not ensure_dlt_config():
        raise AirflowException("Configuration failed")

    with open("/opt/airflow/keys/my-creds.json") as f:
        creds = json.load(f)

    @dlt.resource(
        table_name="nyc_crz_entries",
        write_disposition="replace",
    )
    def crz_entries_data():
        extractor = NYCRZEntriesExtractor()
        last_skip = load_checkpoint() or 0

        # Build initial URL with $skip parameter for pagination
        base_url = (
            f"{API_BASE_URL}?"
            f"$select={SELECT_FIELDS}&"
            f"$orderby=toll_date,toll_hour&"
            f"$top={BATCH_SIZE}&"
            f"$skip={last_skip}"
        )

        url = base_url
        print(f"Starting extraction from skip position: {last_skip}")

        start_time = datetime.utcnow()
        total_records = 0
        batch_count = 0

        try:
            while url:
                if (datetime.utcnow() - start_time) > MAX_RUNTIME - timedelta(minutes=30):
                    print(f"Approaching timeout after {total_records} records. Saving progress...")
                    save_checkpoint(last_skip + total_records)
                    break

                batch, next_url = extractor.fetch_batch(url)
                if not batch:
                    print("No more records to fetch")
                    save_checkpoint(0)  # Reset checkpoint
                    break

                batch_size = len(batch)
                total_records += batch_size
                batch_count += 1
                last_skip += batch_size

                print(
                    f"Batch {batch_count}: Fetched {batch_size} records | "
                    f"Total: {total_records} | Current skip: {last_skip}"
                )

                yield batch
                save_checkpoint(last_skip)

                url = next_url
                if url:
                    time.sleep(1)  # Rate limiting

        except Exception as e:
            print(f"Extraction failed...")
            save_checkpoint(last_skip)
            raise

    try:
        pipeline = dlt.pipeline(
            pipeline_name="nyc_crz_entries_initial",
            destination="filesystem",
            dataset_name="nyc_crz_data",
        )

        load_info = pipeline.run(
            crz_entries_data(),
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
        print(f"Pipeline failed: {str(e)}")
        raise AirflowException(f"Pipeline failed: {str(e)}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=30),
    'max_active_runs': 1,
}

with DAG(
        'nyc_crz_entries_initial_load',
        default_args=default_args,
        schedule_interval=None,
        catchup=False,
        max_active_runs=1,
        tags=['nyc_traffic'],
) as dag:
    load_task = PythonOperator(
        task_id='load_full_dataset',
        python_callable=load_full_dataset,
        execution_timeout=MAX_RUNTIME,
        retries=default_args['retries'],
        retry_delay=default_args['retry_delay'],
    )