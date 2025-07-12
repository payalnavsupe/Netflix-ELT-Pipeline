import hashlib
import os
from airflow.models import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.utils.trigger_rule import TriggerRule
import pandas as pd
import snowflake.connector


# Paths to CSV and hash file
CSV_FILE_PATH = '/path/to/your/Cleaned_NetflixData_CSV.csv'
HASH_FILE_PATH = '/path/to/your/hash_file.txt'

# Function to compute file hash
def compute_file_hash(file_path):
    with open(file_path, 'rb') as f:
        file_bytes = f.read()
        return hashlib.md5(file_bytes).hexdigest()

# Function to check if file has changed
def check_if_file_changed():
    print("Checking if the file has changed...")
    new_hash = compute_file_hash(CSV_FILE_PATH)

    if os.path.exists(HASH_FILE_PATH):
        with open(HASH_FILE_PATH, 'r') as hash_file:
            old_hash = hash_file.read()
        if old_hash == new_hash:
            print("File has not changed. Skipping extraction.")
            return 'skip_task'  # skip extract/load
    print("Changes detected in CSV. Proceeding with extraction.")

    # Save new hash
    with open(HASH_FILE_PATH, 'w') as hash_file:
        hash_file.write(new_hash)
    return 'extract_netflix_data'  # proceed with extract/load

# Step 1: Extract Data
def extract_data():
    print("Extracting data...")
    df = pd.read_csv(CSV_FILE_PATH)
    df.to_csv('/tmp/netflix_extracted.csv', index=False)
    print("Saved extracted data to /tmp/netflix_extracted.csv")

# Step 2: Load Data to Snowflake
def load_to_snowflake():
    print("Connecting to Snowflake...")
    conn = snowflake.connector.connect(
        user='{{ YOUR_USERNAME }}',
        password='{{ YOUR_PASSWORD }}',
        account='{{ YOUR_SNOWFLAKE_ACCOUNT }}',
        warehouse='{{ YOUR_WAREHOUSE }}',
        database='{{ YOUR_DATABASE }}',
        schema='{{ YOUR_SCHEMA }}',
    )
    cs = conn.cursor()
    try:
        print("Uploading CSV to Snowflake stage...")
        cs.execute("""
            PUT file:///tmp/netflix_extracted.csv 
            @%NETFLIX_DATA 
            OVERWRITE = TRUE;
        """)
        print("Copying CSV contents into the table...")
        cs.execute("""
            COPY INTO NETFLIX_DATA
            FROM @%NETFLIX_DATA
            FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);
        """)
        print("Data successfully loaded into Snowflake.")
    finally:
        cs.close()
        conn.close()

# DAG definition
with DAG(
    dag_id='netflix_data_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['netflix', 'project'],
) as dag:

    # Task 0: Check if file has changed
    check_file_task = BranchPythonOperator(
        task_id='check_file_task',
        python_callable=check_if_file_changed,
    )

    # Task 1: Extract CSV
    extract_task = PythonOperator(
        task_id='extract_netflix_data',
        python_callable=extract_data,
    )

    # Task 2: Create Table in Snowflake
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS {{ YOUR_DATABASE }}.{{ YOUR_SCHEMA }}.NETFLIX_DATA (
        show_id STRING,
        type STRING,
        title STRING,
        director STRING,
        country STRING,
        date_added STRING,
        release_year INTEGER,
        duration STRING,
        listed_in STRING,
        rating STRING
    );
    """
    create_table = SnowflakeOperator(
        task_id='create_table_snowflake',
        snowflake_conn_id='snowflake_conn',
        sql=create_table_sql
    )

    # Task 3: Load Data to Snowflake
    load_data = PythonOperator(
        task_id='load_csv_to_snowflake',
        python_callable=load_to_snowflake,
    )

    # Task 4: Run dbt transformation
    run_dbt = BashOperator(
        task_id='run_dbt_transformation',
        bash_command='cd "/path/to/your/netflix_dbt" && dbt run',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    # Dummy skip task
    skip_task = EmptyOperator(task_id='skip_task')

    # Set task dependencies
    check_file_task >> [extract_task, skip_task]
    extract_task >> create_table >> load_data
    [load_data, skip_task] >> run_dbt

