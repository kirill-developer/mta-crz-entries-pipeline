"""
Daily update DAG for NYC Traffic Congestion Data.

This DAG fetches daily congestion pricing data from the NYC Open Data API and loads it to GCS.
It follows a common data pipeline pattern where we process data from the previous day to ensure:
1. Data completeness - we want to ensure we have complete data for a day before processing it
2. Data latency - there might be a delay in data availability from the source
3. Time zone considerations - different time zones might affect when data is considered "complete" for a day

The DAG runs at 7:00 UTC (3:00 AM ET) daily to fetch data from the previous day.
For example, when running on April 16th, it will attempt to fetch data for April 14th:
- execution_date = April 15th (the day before the DAG runs)
- target_date = April 14th (one day before execution_date)

This ensures we have complete data for the day we're processing.
"""

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


def get_latest_available_date():
    """Get the latest date for which data is available in the API"""
    try:
        # Make a simple request to get the most recent data
        base_url = "https://data.ny.gov/api/odata/v4/t6yz-b64h"
        url = f"{base_url}?$select=toll_date&$orderby=toll_date desc&$top=1"
        
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        if data.get('value') and len(data['value']) > 0:
            latest_date_str = data['value'][0]['toll_date']
            # Convert ISO date string to Date object
            latest_date = datetime.strptime(latest_date_str.split('T')[0], '%Y-%m-%d').date()
            print(f"Latest available data date in API: {latest_date}")
            return latest_date
        else:
            print("No data available in the API")
            return None
    except Exception as e:
        print(f"Error checking latest available date: {str(e)}")
        return None


def crz_entries_daily_data(**context):
    """Generator that yields daily CRZ entries data"""
    # Get the latest available date from the API
    latest_date = get_latest_available_date()
    if not latest_date:
        print("No data available to fetch")
        return
    
    # Get the execution date from Airflow context
    execution_date = context.get('execution_date', datetime.now() - timedelta(days=1))
    if isinstance(execution_date, str):
        execution_date = datetime.strptime(execution_date, '%Y-%m-%d').date()
    elif isinstance(execution_date, datetime):
        execution_date = execution_date.date()
    
    # Calculate target date - we want the day before the execution date
    target_date = execution_date - timedelta(days=1)
    
    # If target date is in the future or after the latest available date, skip
    if target_date > datetime.now().date():
        print(f"Skipping fetch for {target_date} - future date")
        return
    
    if target_date > latest_date:
        print(f"Skipping fetch for {target_date} - data not yet available in API (latest available: {latest_date})")
        return
    
    print(f"Fetching data for date: {target_date}")
    
    # Construct the API URL with date filter
    base_url = "https://data.ny.gov/api/odata/v4/t6yz-b64h"
    select_fields = "toll_date,toll_hour,toll_10_minute_block,minute_of_hour,hour_of_day,day_of_week_int,day_of_week,toll_week,time_period,vehicle_class,detection_group,detection_region,crz_entries,excluded_roadway_entries"
    filter_query = f"toll_date eq {target_date}"  # Only get data for the specific date
    order_by = "toll_date,toll_hour"
    
    url = f"{base_url}?$select={select_fields}&$filter={filter_query}&$orderby={order_by}"
    print(f"Fetching URL: {url}")
    
    # Create extractor instance
    extractor = NYCRZEntriesExtractor()
    
    try:
        # Fetch and yield data
        while url:
            batch, next_url = extractor.fetch_batch(url)
            if batch:
                print(f"Fetched {len(batch)} records for date {target_date}")
                yield batch
            else:
                print(f"No new records found for date {target_date}")
            url = next_url
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 400:
            print(f"No data available for date {target_date} - API returned 400 Bad Request")
            return
        raise

def load_daily_updates(**context):
    """Load daily updates from the API to GCS"""
    try:
        # Ensure DLT configuration is set up
        if not ensure_dlt_config():
            raise AirflowException("Configuration failed")

        # Load credentials
        with open("/opt/airflow/keys/my-creds.json") as f:
            creds = json.load(f)

        # Create DLT pipeline
        pipeline = dlt.pipeline(
            pipeline_name="nyc_crz_entries_daily",
            destination="filesystem",
            dataset_name="nyc_crz_data_daily",
        )

        # Run the pipeline with the context
        load_info = pipeline.run(
            crz_entries_daily_data(**context),
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
        schedule_interval='0 7 * * *',  # Run daily at 7:00 AM UTC (3:00 AM ET)
        catchup=False,
        tags=['nyc_traffic'],
) as dag:
    load_task = PythonOperator(
        task_id='load_daily_updates',
        python_callable=load_daily_updates,
        provide_context=True,  # Ensure context is passed to the function
        execution_timeout=timedelta(hours=2),
    )