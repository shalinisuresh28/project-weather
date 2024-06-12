# Project Weather Data Pipeline Documentation

## Overview

This document outlines the design and workflow of the data pipeline for Project Weather, which aims to extract weather data from the Meteomatics API, transform it using dbt, and load it into a Snowflake data warehouse. The process is orchestrated using Azure Managed Airflow.

## Logical Data Model

![Logical Data Model](D:\Weather_Logical_Model-Logical Model.jpg)

## Tech Stack

1. **Orchestration:** Azure Managed Airflow
2. **Storage:** Azure Data Lake Storage (ADLS) Gen2
3. **Transformation:** dbt (data build tool)
4. **Data Warehouse:** Snowflake

## Configurations

Ensure the following configurations are set up in their respective tools:

- **Azure ADLS:**
  - Storage account: `projectweatheradls`
  - Container: `project-weather-data`
  - Directory: `raw`
  
- **Snowflake:**
  - Database: `weather`
  - Schema: `prod_weather_bronze`,`prod_weather_silver`,`prod_weather_gold`
  - User credentials and access configured
  


## Configurations (continued)

- **dbt:**
  - Project.yml configured with model paths and seeds
  - Notification email configured for job status updates
  
- **Azure Managed Airflow:**
  - DAGs stored in the appropriate directory (azure://projectweatheradls.blob.core.windows.net/project-weather-data/dags/)
  - Connections and variables set up for the weather API and ADLS

## Steps Involved

### 1. Connect to the Weather API

- **API:** [Meteomatics Weather API](https://www.meteomatics.com/en/weather-api/)
- **Date Range:** From current date to 10 days from the current date
- **Authentication:** API key required

### 2. Extract and Load Data to ADLS Gen2

- **Data Extraction:**
  - Fetch weather data in JSON format using the API
  - Store the extracted JSON files in ADLS Gen2 at the path: `azure://projectweatheradls.blob.core.windows.net/project-weather-data/raw`
  
### 3. Load Raw Files from ADLS Gen2 to Snowflake Raw Table

- **External Stage in Snowflake:**
  - Create an external stage in Snowflake to access the raw files stored in ADLS Gen2
  - Load the raw JSON data into a Snowflake raw table
  
### 4. Data Transformation using dbt

#### Medallion Architecture

- **Gold Layer:**
  - Dimension Tables: `dim_date`, `dim_location`, `dim_weather_condition`
  - Fact Table: `fct_weather`
  - Seeds:
    - `dim_location`: `seed_location`
    - `dim_weather_condition`: `seed_weather_symbols`
  - Test YAML files configured for each table

- **Silver Layer:**
  - Intermediate Table: `int_weather`

#### Parsing and Transformation

- **Parse JSON Data:**
  - Extract relevant data from the raw JSON files and load it into the `int_weather` table in a structured format
  
- **Transform Data:**
  - The transformation for the `fct_weather` table is designed to aggregate and structure weather data from the intermediate `int_weather` table. The transformation process involves extracting relevant parameters, grouping the data, and generating surrogate keys for unique identification.
  - The transformation process involves the following key steps:
        1. **Extract and Pivot Data:** The `transformed` CTE pivots weather parameters into individual columns and aggregates the data by date and location.
        2. **Generate Surrogate Keys:** The `final` CTE creates unique identifiers (`Weather_Id` and `Location_Id`) for each row.\
                    Generating surrogate keys for unique identification:
                      - `Weather_Id`: Generated using `dbt_utils.generate_surrogate_key` based on `Date`, `Latitude`, and `Longitude`.
                      - `Location_Id`: Generated using `dbt_utils.generate_surrogate_key` based on `Latitude` and `Longitude`.
        3. **Prepare Final Dataset:** The final select statement prepares the transformed data for loading into the `fct_weather` table.

### 5. Orchestrate Workflow using Azure Managed Airflow

- **DAG Workflow:**
  - `extract_and_upload_json_task` -> `load_to_snowflake_task` -> `trigger_dbt_cloud_job_task`
  
  ## Summary of the DAG Workflow
    1. **Data Extraction and Upload:**
    - Extract weather data from the API.
    - Upload JSON files to ADLS Gen2.

    2. **Data Loading into Snowflake:**
    - Load raw JSON data from ADLS Gen2 into a Snowflake raw table.

    3. **Data Transformation with dbt:**
    - Trigger dbt cloud job to transform data.
    - Load transformed data into dimension and fact tables in Snowflake.

    4. **Notification:**
    - Send the status of the dbt job to the configured email address.
 
### 6. Automated Workflow

All the above-mentioned steps are automated using Azure Managed Airflow. The DAG is prepared to execute the tasks in sequence, ensuring a seamless flow from data extraction to transformation and loading.

## Conclusion

This pipeline enables automated extraction of weather data from the Meteomatics API, transforming it using dbt, and loading it into Snowflake for further analysis. The entire process is orchestrated using Azure Managed Airflow, ensuring efficient and reliable data processing.
