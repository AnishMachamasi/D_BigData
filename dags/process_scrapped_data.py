from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.utils.dates import days_ago
import pandas as pd
import boto3
from io import StringIO
from sqlalchemy import create_engine
import os
from datetime import timedelta, datetime

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'process_scrapped_data',
    start_date=datetime(2023, 11, 19),
    default_args=default_args,
    description='Extract data from MinIO, process it, and load it into PostgreSQL',
    schedule=None
)

def fetch_data_from_minio(**kwargs):
    # MinIO configuration
    minio_endpoint = 'http://myminio:9000'
    minio_access_key = 'minio'
    minio_secret_key = 'minio123'
    bucket_name = 'scraped-data'
    file_name = 'Scraped_Data.csv'

    # Initialize MinIO client
    s3 = boto3.client('s3',
                       endpoint_url=minio_endpoint,
                       aws_access_key_id=minio_access_key,
                       aws_secret_access_key=minio_secret_key,
                       region_name='us-east-1')

    # Read data from MinIO
    obj = s3.get_object(Bucket=bucket_name, Key=file_name)
    data = obj['Body'].read().decode('utf-8')
    
    # Save data to a local file
    with open('/opt/airflow/dags/custom_pandas/data/Scraped_Data.csv', 'w') as file:
        file.write(data)

def process_data(**kwargs):
    # Load data into a DataFrame
    df = pd.read_csv('/opt/airflow/dags/custom_pandas/data/Scraped_Data.csv')

    # Convert 'Product Price' to numeric
    df['Product Price'] = df['Product Price'].replace({'₹': '', ',': ''}, regex=True).astype(float)

    # Ensure 'Rating' is treated as string before extracting numeric values
    df['Rating'] = df['Rating'].astype(str).str.extract(r'(\d+\.\d+)').astype(float)

    # Convert 'Number of reviews' to string to handle replacement
    df['Number of reviews'] = df['Number of reviews'].astype(str).str.replace(',', '', regex=False).astype(int)

    # Save the processed data to a local file
    df.to_csv('/opt/airflow/dags/custom_pandas/data/Processed_Data.csv', index=False)

def load_data_to_postgres(**kwargs):
    # PostgreSQL configuration
    database_url = 'postgresql://airflow:airflow@postgres:5432/scrapdb'

    # Create a SQLAlchemy engine
    engine = create_engine(database_url)

    # Load processed data into PostgreSQL
    df = pd.read_csv('/opt/airflow/dags/custom_pandas/data/Processed_Data.csv')
    df.to_sql('products', engine, if_exists='replace', index=False)

# Define the tasks
fetch_data = PythonOperator(
    task_id='fetch_data_from_minio',
    python_callable=fetch_data_from_minio,
    dag=dag,
)

process_data = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    dag=dag,
)

load_data = PythonOperator(
    task_id='load_data_to_postgres',
    python_callable=load_data_to_postgres,
    dag=dag,
)

# Define task dependencies
fetch_data >> process_data >> load_data
