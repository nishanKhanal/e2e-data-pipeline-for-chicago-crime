from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv
import sys
import os

sys.path.append("/home/test/Documents/Workspace/ms-in-data-science-and-analytics/CIS 660/project/")

from etl.extract import extract
from etl.transform import transform
from etl.load import load
from etl.logger import setup_logging


# Database config — optionally use Airflow Variables or Secrets Backend in production
load_dotenv()


DB_CONFIG = {
    'host': os.getenv("DB_HOST"),
    'user': os.getenv("DB_USER"),
    'password': os.getenv("DB_PASS"),
    'dbname': os.getenv("DB_NAME"),
    'port': os.getenv("DB_PORT")
}


# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}

# Define DAG
dag = DAG(
    dag_id='chicago_crime_etl',
    default_args=default_args,
    description='ETL DAG for updating Chicago Crime Data daily',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1
)

# Task 1: Extract from API
def extract_task(**kwargs):
    setup_logging()
    app_token = "y6ET06DOXVhBlMOsL6XdVT3jk"
    data = extract(db_config=DB_CONFIG, app_token=app_token)
    kwargs['ti'].xcom_push(key='extracted_data', value=data)

# Task 2: Transform

def transform_task(**kwargs):
    import pandas as pd
    setup_logging()
    raw_data = kwargs['ti'].xcom_pull(key='extracted_data', task_ids='extract')
    df = pd.DataFrame(raw_data)
    df = transform(df)
    kwargs['ti'].xcom_push(key='transformed_df', value=df.to_json(orient='records', date_format="iso" ))

# Task 3: Load

def load_task(**kwargs):
    import pandas as pd
    setup_logging()
    data_json = kwargs['ti'].xcom_pull(key='transformed_df', task_ids='transform')
    df = pd.read_json(data_json, orient='records')
    load(df, DB_CONFIG)

# Airflow Operators
extract_op = PythonOperator(
    task_id='extract',
    python_callable=extract_task,
    provide_context=True,
    dag=dag
)

transform_op = PythonOperator(
    task_id='transform',
    python_callable=transform_task,
    provide_context=True,
    dag=dag
)

load_op = PythonOperator(
    task_id='load',
    python_callable=load_task,
    provide_context=True,
    dag=dag
)

# DAG Execution Order
extract_op >> transform_op >> load_op
