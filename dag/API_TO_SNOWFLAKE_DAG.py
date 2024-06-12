from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import snowflake.connector
import requests
import os
import json
from azure.storage.blob import BlobServiceClient

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'API_TO_SNOWFLAKE_DAG',
    default_args=default_args,
    description='Fetch weather data from API, upload to Azure Data Lake in JSON format, trigger the dbt cloud job and load the transformed data into Snowflake',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
)

# Fetch Airflow Variables
SNOWFLAKE_ACCOUNT = Variable.get('snowflake_account')
SNOWFLAKE_USER = Variable.get('snowflake_user')
SNOWFLAKE_PASSWORD = Variable.get('snowflake_password')
SNOWFLAKE_DATABASE = Variable.get('snowflake_database')
SNOWFLAKE_SCHEMA = Variable.get('snowflake_schema')
SNOWFLAKE_WAREHOUSE = Variable.get('snowflake_warehouse')
AZURE_STORAGE_CONNECTION_STRING = Variable.get('azure_storage_connection_string')
AZURE_CONTAINER_NAME = Variable.get('azure_container_name')

# Task 1: Start task
start_task = DummyOperator(task_id='start_task', dag=dag)

# Task 2: Extract data from API and upload JSON files to ADLS
def extract_and_upload_json():
    try:
        ctx = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )
        print("Connected to Snowflake successfully.")
    except Exception as e:
        print(f"Failed to connect to Snowflake: {e}")
        raise

    start_date = datetime.now()
    end_date = start_date + timedelta(days=10)
    start_date_str = start_date.strftime('%Y-%m-%dT00:00:00.000+02:00')
    end_date_str = end_date.strftime('%Y-%m-%dT00:00:00.000+02:00')
    location_query = "SELECT latitude, longitude, city FROM dim_location"

    try:
        cur = ctx.cursor()
        cur.execute(location_query)
        locations = cur.fetchall()
        print(f"Fetched {len(locations)} locations from Snowflake.")
    except Exception as e:
        print(f"Failed to execute query or fetch data: {e}")
        cur.close()
        ctx.close()
        raise

    try:
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
        container_client = blob_service_client.get_container_client(AZURE_CONTAINER_NAME)
        print("Connected to Azure Blob Storage successfully.")
    except Exception as e:
        print(f"Failed to connect to Azure Blob Storage: {e}")
        raise

    for location in locations:
        latitude, longitude, city = location
        api_url = f"https://api.meteomatics.com/{start_date_str}--{end_date_str}:P1D/wind_speed_10m:ms,wind_dir_10m:d,t_min_2m_24h:C,t_max_2m_24h:C,precip_24h:mm,weather_symbol_24h:idx,uv:idx,sunrise:sql,sunset:sql/{latitude},{longitude}/json?model=mix"
        
        try:
            response = requests.get(api_url, auth=('bestseller_sureshbabu_shalini', 'O1oLez63T4'))
            
            if response.status_code == 200:
                data = response.json()
                file_name = f"{city.replace(' ', '_')}.json"
                with open(file_name, 'w') as json_file:
                    json.dump(data, json_file)
                
                blob_client = container_client.get_blob_client(file_name)
                with open(file_name, "rb") as data_file:
                    blob_client.upload_blob(data_file, overwrite=True)
                
                os.remove(file_name)
                print(f"Successfully processed and uploaded data for {city}")
            else:
                print(f"Failed to fetch data for location ({latitude}, {longitude}). Status code: {response.status_code}")
        except Exception as e:
            print(f"Error processing location ({latitude}, {longitude}): {e}")

    cur.close()
    ctx.close()
    print("Closed Snowflake connection.")

extract_and_upload_json_task = PythonOperator(
    task_id='extract_and_upload_json',
    python_callable=extract_and_upload_json,
    dag=dag,
)

# Task 3: Load data to Snowflake RAW
AZURE_SAS_TOKEN = Variable.get('azure_sas_token')

create_table_raw = "CREATE OR REPLACE TABLE WEATHER.PROD_WEATHER_BRONZE.RAW_WEATHER (COLUMN_1 VARIANT);"
create_stage = f"""
CREATE OR REPLACE STAGE WEATHER.PROD_WEATHER_BRONZE.WEATHER_ADLS_STAGE 
URL = 'azure://projectweatheradls.blob.core.windows.net/project-weather-data/raw' 
CREDENTIALS = ( AZURE_SAS_TOKEN = '{AZURE_SAS_TOKEN}' )  
DIRECTORY = ( ENABLE = true );
"""
create_file_format = "CREATE OR REPLACE FILE FORMAT WEATHER.PROD_WEATHER_BRONZE.JSON_FILE_FORMAT TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE;"
load_data_to_raw = """
COPY INTO WEATHER.PROD_WEATHER_BRONZE.RAW_WEATHER 
from @WEATHER.PROD_WEATHER_BRONZE.WEATHER_ADLS_STAGE
FILE_FORMAT = (FORMAT_NAME = 'WEATHER.PROD_WEATHER_BRONZE.JSON_FILE_FORMAT')
pattern = '.*.json';
"""
drop_stage = "DROP STAGE WEATHER.PROD_WEATHER_BRONZE.WEATHER_ADLS_STAGE;"

def load_to_snowflake_raw():
    try:
        ctx = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )
        print("Connected to Snowflake successfully.")
        
        cur = ctx.cursor()
        cur.execute(create_table_raw)
        print("Raw Table Created")
        cur.execute(create_stage)
        print("Stage Created")
        cur.execute(create_file_format)
        print("File Format Created")
        cur.execute(load_data_to_raw)
        print("Data Loaded to Raw")
        cur.execute(drop_stage)
        print("Stage Dropped")
        
    except Exception as e:
        print(f"Error loading data to Snowflake RAW: {e}")
        raise
    
    finally:
        ctx.close()
        print("Closed Snowflake connection.")

load_to_snowflake_task = PythonOperator(
    task_id='load_to_snowflake_raw',
    python_callable=load_to_snowflake_raw,
    dag=dag,
)

# Task 4: Trigger dbt cloud job
def trigger_dbt_cloud_job(**kwargs):
    account_id = kwargs.get('account_id')
    job_id = kwargs.get('job_id')
    api_key_var = Variable.get('dbt_api_key')
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key_var}"
    }
    body = {"cause": "Triggered via Airflow"}

    url = f"https://pf524.us1.dbt.com/api/v2/accounts/{account_id}/jobs/{job_id}/run/"
    
    try:
        response = requests.post(url, headers=headers, data=json.dumps(body))
        response.raise_for_status()
        return response.json().get('id')
    except Exception as e:
        print(f"Failed to trigger dbt Cloud job: {e}")
        raise

trigger_dbt_cloud_job_task = PythonOperator(
    task_id='trigger_dbt_cloud_job',
    python_callable=trigger_dbt_cloud_job,
    op_kwargs={
        'account_id': '70403103930533',
        'job_id': '70403103939313',
    },
    dag=dag,
)

# Task 5: End Task
end_task = DummyOperator(task_id='end_task', dag=dag)

# Set task dependencies
start_task >> extract_and_upload_json_task >> load_to_snowflake_task >> trigger_dbt_cloud_job_task >> end_task
