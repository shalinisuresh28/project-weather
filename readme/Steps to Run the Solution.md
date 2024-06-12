# Steps to Run the Solution 

## Overview

This guide provides step-by-step instructions to run the Project Weather data pipeline. The pipeline extracts weather data from the Meteomatics API, transforms it using dbt, and loads it into a Snowflake data warehouse, orchestrated by Azure Managed Airflow.

## Prerequisites

- Access to Azure Managed Airflow
- Access to Snowflake

## Step-by-Step Instructions

### 1. Log in to Azure Managed Airflow

1. Open your web browser and navigate to [Azure Managed Airflow](https://fd701914951718.westeurope.airflow.svc.datafactory.azure.com/home).
2. Log in using the following credentials:
   - **Username:** admin
   - **Password:** admin123

### 2. Trigger the DAG

1. In the Airflow UI, locate the DAG named `API_TO_SNOWFLAKE_DAG`.
2. Click on the toggle button to enable the DAG if it is not already enabled.
3. Click on the play button to trigger the DAG manually.

The DAG will execute the following sequence of tasks:
- `extract_and_upload_json_task`: Extracts weather data from the Meteomatics API and uploads JSON files to ADLS Gen2.
- `load_to_snowflake_task`: Loads the raw JSON files from ADLS Gen2 into a Snowflake raw table.
- `trigger_dbt_cloud_job_task`: Triggers a dbt cloud job to transform and load the data into the Snowflake `Prod_Weather_gold` schema.

### 3. Monitor the DAG Execution

1. In the Airflow UI, navigate to the **DAG Runs** tab.
2. Monitor the status of the tasks. Ensure that all tasks complete successfully.
3. If any task fails, review the logs to diagnose and resolve the issue.

### 4. Log in to Snowflake

1. Open your web browser and navigate to the [Snowflake Dedicated Login URL](https://fnzcqvd-le43064.snowflakecomputing.com).
2. Log in using the following credentials:
   - **Username:** SHALINISURESHBABU28
   - **Password:** Admin@12345

### 5. Verify the Data in Snowflake

1. Once logged in, navigate to the `weather` database.
2. Switch to the `Prod_Weather_gold` schema.
3. Verify the presence of the following tables:
   - **Dimension Tables:**
     - `dim_date`
     - `dim_location`
     - `dim_weather_condition`
   - **Fact Table:**
     - `fct_weather`
4. You can run SQL queries to check the data within these tables and ensure the transformation and loading processes have been executed correctly.

### 6. Review dbt Cloud Job Status (Optional)

1. Open your web browser and navigate to [dbt Cloud](https://pf524.us1.dbt.com/).
2. Log in using the following credentials:
   - **Username:** shlnsureshbabu@gmail.com
   - **Password:** admin@12345
3. Navigate to the job triggered by the Airflow DAG.
4. Review the job status and logs to ensure the transformations were successful.

### Conclusion

By following these steps, you can run the Project Weather data pipeline to extract, transform, and load weather data into Snowflake. This automated process is orchestrated using Azure Managed Airflow, ensuring a seamless workflow from data extraction to transformation and loading.
