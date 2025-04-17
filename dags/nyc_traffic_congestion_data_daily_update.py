"""
Daily update DAG for NYC Traffic Congestion Data.

This DAG ensures data reliability by:
1. Verifying the existence and validity of historical data
2. Performing data quality checks
3. Maintaining proper incremental updates

The pipeline includes:
- Historical data verification (size and record count checks)
- Proper error handling and retries
- Data quality monitoring
"""

from datetime import datetime, timedelta, date
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
from google.cloud import bigquery, storage
from google.oauth2 import service_account

# Constants
CHECKPOINT_DIR = "/opt/airflow/data/checkpoints"
CHECKPOINT_FILE = f"{CHECKPOINT_DIR}/nyc_crz_entries_daily_checkpoint.json"
API_BASE_URL = "https://data.ny.gov/api/odata/v4/t6yz-b64h"
SELECT_FIELDS = "toll_date,toll_hour,toll_10_minute_block,minute_of_hour,hour_of_day,day_of_week_int,day_of_week,toll_week,time_period,vehicle_class,detection_group,detection_region,crz_entries,excluded_roadway_entries"
HISTORICAL_LOAD_PATH = "nyc_crz_data/nyc_crz_entries"
DAILY_LOAD_PATH = "nyc_crz_data_daily/nyc_crz_entries_daily"
MIN_HISTORICAL_FILE_SIZE = 1024 * 1024  # 1MB minimum size for historical data
MIN_DAILY_RECORDS = 100  # Minimum expected records per day

# Ensure checkpoint directory exists
Path(CHECKPOINT_DIR).mkdir(parents=True, exist_ok=True)

def get_bigquery_client():
    """Get authenticated BigQuery client"""
    with open("/opt/airflow/keys/my-creds.json") as f:
        creds = json.load(f)
    
    return bigquery.Client(
        credentials=service_account.Credentials.from_service_account_info(
            creds,
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        ),
        project=creds['project_id']
    )

def verify_historical_data():
    """Verify historical data exists and is valid"""
    try:
        # Check BigQuery data
        client = get_bigquery_client()
        query = """
        SELECT 
            COUNT(*) as record_count,
            MIN(toll_date) as min_date,
            MAX(toll_date) as max_date
        FROM `nyctrafficanalysis.nyc_traffic_nyc_traffic.fact_traffic`
        """
        
        results = client.query(query).result()
        row = list(results)[0]
        
        if row.record_count < MIN_DAILY_RECORDS:
            print(f"Warning: Historical data has only {row.record_count} records")
            return False
            
        print(f"Historical data verified: {row.record_count} records from {row.min_date} to {row.max_date}")
        return True
        
    except Exception as e:
        print(f"Error verifying historical data: {str(e)}")
        return False

def get_latest_available_date():
    """Get the latest date for which data is available in the API"""
    try:
        url = f"{API_BASE_URL}?$select=toll_date&$orderby=toll_date desc&$top=1"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        if data.get('value') and len(data['value']) > 0:
            latest_date_str = data['value'][0]['toll_date']
            latest_date = datetime.strptime(latest_date_str.split('T')[0], '%Y-%m-%d').date()
            print(f"Latest available data date in API: {latest_date}")
            return latest_date
        else:
            print("No data available in API")
            return None
    except Exception as e:
        print(f"Error checking latest available date: {str(e)}")
        return None

def get_latest_processed_date():
    """Get the latest date we've processed from BigQuery"""
    try:
        client = get_bigquery_client()
        # First check staging table for most recent data
        query = """
        SELECT MAX(toll_date) as latest_date
        FROM `nyctrafficanalysis.nyc_traffic.congestion_pricing_entries`
        """
        
        results = client.query(query).result()
        row = list(results)[0]
        
        if row.latest_date:
            # Convert to date if it's a datetime
            staging_date = row.latest_date.date() if isinstance(row.latest_date, datetime) else row.latest_date
            print(f"Latest date in staging: {staging_date}")
        else:
            staging_date = None
            
        # Then check fact table
        query = """
        SELECT MAX(toll_date) as latest_date
        FROM `nyctrafficanalysis.nyc_traffic_nyc_traffic.fact_traffic`
        """
        
        results = client.query(query).result()
        row = list(results)[0]
        
        if row.latest_date:
            # Convert to date if it's a datetime
            fact_date = row.latest_date.date() if isinstance(row.latest_date, datetime) else row.latest_date
            print(f"Latest date in fact table: {fact_date}")
        else:
            fact_date = None
        
        # Return the most recent date from either table
        if staging_date and fact_date:
            return max(staging_date, fact_date)
        return staging_date or fact_date or None
        
    except Exception as e:
        print(f"Error getting latest processed date: {str(e)}")
        return None

def get_daily_data(date_obj):
    """
    Fetch data for a specific date from the API.
    Args:
        date_obj (datetime.date): The date to fetch data for
    Returns:
        list: List of records for the date, or None if no data available
    """
    try:
        # Format date for simple date equality check
        formatted_date = date_obj.strftime("%Y-%m-%d")
        all_records = []
        skip = 0
        
        while True:
            filter_query = f"$filter=toll_date eq '{formatted_date}'"
            url = f"{API_BASE_URL}?$select={SELECT_FIELDS}&{filter_query}&$orderby=toll_hour&$skip={skip}"
            
            print(f"Fetching data from URL with skip={skip}: {url}")
            
            # Set up session with retries
            session = requests.Session()
            retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
            session.mount('https://', HTTPAdapter(max_retries=retries))
            
            response = session.get(url)
            response.raise_for_status()
            data = response.json()
            
            batch = data.get('value', [])
            if not batch:  # No more records
                break
                
            all_records.extend(batch)
            if len(batch) < 1000:  # Last page
                break
                
            skip += len(batch)
            print(f"Retrieved {len(all_records)} records so far for {date_obj}")
        
        if not all_records:
            print(f"No data available for {date_obj}")
            return None
        
        print(f"Successfully fetched {len(all_records)} total records for {date_obj}")
        print(f"Sample record: {all_records[0]}")
        
        return all_records
        
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 400:
            print(f"Invalid date format or no data available for {date_obj}")
            print(f"API Error Response: {e.response.text}")
            return None
        raise AirflowException(f"API request failed for {date_obj}: {str(e)}")
    except Exception as e:
        print(f"Error fetching data for {date_obj}: {str(e)}")
        return None

def get_partition_path(date_obj):
    """Generate partition path for a given date"""
    return f"year={date_obj.year}/month={date_obj.month:02d}/day={date_obj.day:02d}"

def process_daily_batch(date_obj, records, pipeline):
    """Process a single day's batch of data"""
    if not records:
        print(f"No data available for {date_obj}")
        return None

    # Generate partition path and filename
    partition_path = get_partition_path(date_obj)
    filename = f"nyc_crz_entries_{date_obj.strftime('%Y%m%d')}"
    
    print(f"Processing batch for {date_obj} with {len(records)} records")
    print(f"Writing to partition: {partition_path}")
    
    # Load the batch - use nyc_crz_data_daily as the base path
    load_info = pipeline.run(
        [records],
        loader_file_format="parquet",
        write_disposition="replace",  # Each day gets its own file
        dataset_name="nyc_crz_data_daily",  # Base path
        table_name=f"{partition_path}/{filename}",  # Include partition path in table name
    )
    
    return load_info

def load_daily_updates(**context):
    """Load daily updates with enhanced error handling and verification"""
    try:
        # Verify historical data exists and is valid
        if not verify_historical_data():
            raise AirflowException("Historical data verification failed")

        # Load credentials
        with open("/opt/airflow/keys/my-creds.json") as f:
            creds = json.load(f)

        # Set up environment for dlt
        os.environ['DESTINATION__FILESYSTEM__BUCKET_URL'] = Variable.get(
            "DLT_FILESYSTEM_BUCKET_URL",
            default_var="gs://terraform-nyctrafficanalysis-bucket"
        )
        os.environ['DESTINATION__FILESYSTEM__CREDENTIALS__PROJECT_ID'] = creds['project_id']
        os.environ['DESTINATION__FILESYSTEM__CREDENTIALS__PRIVATE_KEY'] = creds['private_key']
        os.environ['DESTINATION__FILESYSTEM__CREDENTIALS__CLIENT_EMAIL'] = creds['client_email']
        os.environ['DESTINATION__FILESYSTEM__CREDENTIALS__TOKEN_URI'] = creds['token_uri']

        # Create pipeline for daily updates
        pipeline = dlt.pipeline(
            pipeline_name="nyc_crz_entries_daily",
            destination="filesystem",
            full_refresh=False
        )

        # Get latest processed and available dates
        latest_processed = get_latest_processed_date()
        if latest_processed is None:
            latest_processed = date(2025, 1, 1)
        
        latest_available = get_latest_available_date()
        if latest_available is None:
            raise AirflowException("Could not determine latest available date from API")

        if latest_processed >= latest_available:
            print(f"No new data to process. Latest processed: {latest_processed}, Latest available: {latest_available}")
            return None

        # Process each day's batch separately
        load_results = []
        current_date = latest_processed + timedelta(days=1)
        
        while current_date <= latest_available:
            try:
                # Get data for current date
                records = get_daily_data(current_date)
                if records:
                    # Process the batch
                    load_info = process_daily_batch(current_date, records, pipeline)
                    if load_info:
                        load_results.append({
                            'date': current_date.strftime('%Y-%m-%d'),
                            'records': len(records),
                            'load_info': load_info
                        })
            except Exception as e:
                print(f"Error processing batch for {current_date}: {str(e)}")
                # Continue with next batch instead of failing entire pipeline
                
            current_date += timedelta(days=1)

        if not load_results:
            print("No new data was loaded")
            return None

        print(f"Daily load completed. Processed {len(load_results)} batches:")
        for result in load_results:
            print(f"Date: {result['date']}, Records: {result['records']}")
        
        return load_results

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
        provide_context=True,
        execution_timeout=timedelta(hours=2),
    )