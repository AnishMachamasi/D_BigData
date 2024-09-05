from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

from utils import print_hello

import logging
import json

from datetime import datetime, timedelta

# Define a Python function for the new task
def fetch_data_from_mysql():
    logging.info("Fetching data from MySQL...")
    mysql_hook = MySqlHook(mysql_conn_id='mysql_conn')
    sql = "SELECT * FROM exchange_rates LIMIT 10;"
    results = mysql_hook.get_records(sql)
    logging.info(f"Query results: {results}")

# # Define a simple Python function to print a message
# def print_hello():
#     print("Hello, this is a sample Python job!")

# Define default arguments for the DAG
default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "anishmachamasi2262@gmail.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

# Instantiate the DAG
dag = DAG(
    "data_pipeline_spark",
    start_date=datetime(2023, 11, 19),
    default_args=default_args,
    schedule=None
)

# Define the tasks
start = DummyOperator(
    task_id="start",
    dag=dag
)

# Running Spark Job to process the data
forex_processing1 = SparkSubmitOperator(
    task_id="forex_processing1",
    conn_id="spark_conn",
    application="/opt/airflow/dags/scripts/test.py",
    verbose=False,
    dag=dag
)

# Running Spark Job to process the data
forex_processing2 = SparkSubmitOperator(
    task_id="forex_processing2",
    conn_id="spark_conn",
    application="/opt/airflow/dags/scripts/test2.py",
    verbose=False,
    dag=dag
)

# Running Spark Job to process the data
forex_processing2 = SparkSubmitOperator(
    task_id="forex_processing3",
    conn_id="spark_conn",
    application="/opt/airflow/dags/scripts/test.py",
    verbose=False,
    dag=dag
)

# Run a Python job
run_python_job = PythonOperator(
    task_id="run_sample_python_job",
    python_callable=print_hello,
    dag=dag
)

# Fetch data from MySQL
fetch_mysql_data = PythonOperator(
    task_id="fetch_mysql_data",
    python_callable=fetch_data_from_mysql,
    dag=dag
)

# Define task dependencies
start >> [forex_processing1, forex_processing2, run_python_job]

# forex_processing1 and forex_processing2 can run in parallel
forex_processing1 >> fetch_mysql_data
forex_processing2 >> fetch_mysql_data
