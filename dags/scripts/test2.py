from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("PostgresConnection") \
    .getOrCreate()

url = "jdbc:postgresql://postgres:5432/airflow"
table = "connection"
properties = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver"
}