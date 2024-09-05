from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.models import Variable
import pandas as pd
import os
import sys
import logging
import pandas as pd
import boto3
from io import StringIO
from sqlalchemy import create_engine

from custom_pandas.circuits import extract_circuits
from custom_pandas.races import extract_races
from custom_pandas.transform_and_load import transform_data, load_data

# Add the directory containing custom_pandas to the Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Define a Python function for the new task
def fetch_data_from_mysql():
    logging.info("Fetching data from MySQL...")
    mysql_hook = MySqlHook(mysql_conn_id='mysql_conn')
    sql = "SELECT * FROM exchange_rates LIMIT 10;"
    results = mysql_hook.get_records(sql)
    logging.info(f"Query results: {results}")

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'data_pipeline_using_pandas',
    default_args=default_args,
    description='A simple DAG to process F1 data',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 8, 1),
    catchup=False,
)

# Define the tasks
start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)

extract_circuits_task = PythonOperator(
    task_id='extract_circuits',
    python_callable=extract_circuits,
    op_kwargs={'circuits_file_path': '/opt/airflow/dags/custom_pandas/data/circuits.csv'},
    provide_context=True,
    dag=dag,
)

extract_races_task = PythonOperator(
    task_id='extract_races',
    python_callable=extract_races,
    op_kwargs={'races_file_path': '/opt/airflow/dags/custom_pandas/data/races.csv'},
    provide_context=True,
    dag=dag,
)

transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

load_data_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    provide_context=True,
    dag=dag,
)

# Fetch data from MySQL
fetch_mysql_data = PythonOperator(
    task_id="fetch_mysql_data",
    python_callable=fetch_data_from_mysql,
    dag=dag
)

# Define the task dependencies
start >> [extract_circuits_task, extract_races_task] >> transform_data_task >> load_data_task >> fetch_mysql_data >> end