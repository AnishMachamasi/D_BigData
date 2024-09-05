from datetime import datetime, timedelta
from airflow.hooks.postgres_hook import PostgresHook
from sqlalchemy import create_engine
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
import yfinance as yf
import pandas as pd
import json
from confluent_kafka import Producer, Consumer
from minio import Minio
from minio.error import S3Error
import io
import os
import time

# MinIO configuration
MINIO_ENDPOINT = 'myminio:9000'  # Replace with your MinIO endpoint
MINIO_ACCESS_KEY = 'minio'    # Replace with your MinIO access key
MINIO_SECRET_KEY = 'minio123'    # Replace with your MinIO secret key
MINIO_BUCKET_NAME = 'kafka'   # Replace with your bucket name

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 15),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

# Define the DAG
dag = DAG(
    'stock_data_pipeline',
    start_date=datetime(2023, 11, 19),
    default_args=default_args,
    description='Extract data from MinIO, process it, and load it into PostgreSQL',
    schedule=None
)

def fetch_stock_data(ticker, start_date, end_date):
    stock = yf.Ticker(ticker)
    data = stock.history(start=start_date, end=end_date)
    return data

def send_data_to_kafka(**kwargs):
    ticker = kwargs['ticker']
    start_date = kwargs['start_date']
    end_date = kwargs['end_date']
    
    data = fetch_stock_data(ticker, start_date, end_date)
    data = data.reset_index()
    data.rename(columns={'index': 'Date'}, inplace=True)
    
    conf = {'bootstrap.servers': 'kafka:9092'}  # Replace with your Kafka broker addresses
    producer = Producer(conf)

    def delivery_report(err, msg):
        if err is not None:
            print(f"Delivery failed: {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}]")
            
    for index, row in data.iterrows():
        message = row.to_dict()
        for key, value in message.items():
            if isinstance(value, pd.Timestamp):
                message[key] = value.isoformat()
        
        producer.produce(f'{ticker}_stock', json.dumps(message), callback=delivery_report)
        producer.poll(1)

    producer.flush()

def consume_data_from_kafka(**kwargs):
    ticker = kwargs['ticker']
    conf = {
        'bootstrap.servers': 'kafka:9092',
        'group.id': 'my-group',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(conf)
    consumer.subscribe([f'{ticker}_stock'])

    empty_time_threshold = 5  # Time to wait for messages in seconds
    last_message_time = time.time()
    
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                # No message received; check if the timeout has been reached
                if time.time() - last_message_time > empty_time_threshold:
                    print("No new messages for a while, stopping the consumer.")
                    break
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue
            
            last_message_time = time.time()
            message_value = msg.value().decode('utf-8')
            try:
                json_data = json.loads(message_value)
                save_to_minio(ticker, json_data)
            except json.JSONDecodeError as e:
                print(f"Failed to decode JSON: {e}")

    except KeyboardInterrupt:
        print("Consuming interrupted.")
    finally:
        consumer.close()

def save_to_minio(stock_name, data):
    file_name = f'{stock_name}.csv'
    
    try:
        try:
            response = minio_client.get_object(MINIO_BUCKET_NAME, file_name)
            df_existing = pd.read_csv(io.BytesIO(response.read()))
        except S3Error as e:
            if e.code == 'NoSuchKey':
                df_existing = pd.DataFrame()
            else:
                raise
        
        df_new = pd.DataFrame([data])
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
        
        temp_file_path = f'/tmp/{file_name}'
        df_combined.to_csv(temp_file_path, index=False)
        
        minio_client.fput_object(
            MINIO_BUCKET_NAME,
            file_name,
            temp_file_path
        )
        print(f"Uploaded {temp_file_path} to MinIO bucket {MINIO_BUCKET_NAME}")

    except S3Error as e:
        print(f"Error handling file in MinIO: {e}")

def process_data_from_minio(**kwargs):
    ticker = kwargs['ticker']
    file_name = f'{ticker}.csv'
    processed_table_name = f'processed_{ticker}'  # Define table name for processed data

    # Use Airflow's PostgresHook to get connection
    postgres_hook = PostgresHook(postgres_conn_id='postgres_conn')
    conn = postgres_hook.get_conn()

    # Create SQLAlchemy engine
    engine = create_engine(postgres_hook.get_uri())

    try:
        # Download the file from MinIO
        response = minio_client.get_object(MINIO_BUCKET_NAME, file_name)
        df = pd.read_csv(io.BytesIO(response.read()))

        # Apply preprocessing rules
        df['Date'] = pd.to_datetime(df['Date'])
        df.set_index('Date', inplace=True)
        df['Daily_Return'] = df['Close'].pct_change()
        df.dropna(inplace=True)

        # Save the processed data to PostgreSQL
        df.to_sql(processed_table_name, engine, if_exists='replace', index=True, method='multi')
        print(f"Uploaded processed data to PostgreSQL table {processed_table_name}")

    except S3Error as e:
        print(f"Error processing file in MinIO: {e}")
    except Exception as e:
        print(f"Error saving data to PostgreSQL: {e}")
        
# Define tasks
start = DummyOperator(task_id='start', dag=dag)

fetch_and_send = PythonOperator(
    task_id='fetch_and_send_to_kafka',
    python_callable=send_data_to_kafka,
    op_kwargs={'ticker': 'AAPL', 'start_date': '2023-01-01', 'end_date': '2023-12-31'},
    dag=dag
)

consume_and_save = PythonOperator(
    task_id='consume_from_kafka_and_save',
    python_callable=consume_data_from_kafka,
    op_kwargs={'ticker': 'AAPL'},
    dag=dag
)

process_data = PythonOperator(
    task_id='process_data_from_minio',
    python_callable=process_data_from_minio,
    op_kwargs={'ticker': 'AAPL'},
    dag=dag
)

end = DummyOperator(task_id='end', dag=dag)

# Define task dependencies
start >> fetch_and_send >> consume_and_save >> process_data >> end