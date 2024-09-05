# from airflow.providers.amazon.aws.hooks.s3 import S3Hook
# import logging

# bucket= 'raw'
# hook = S3Hook(aws_conn_id = 'minio_s3_conn')

# readKey = hook.read_key(key = 'customers.json', bucket_name = bucket)

# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
# import json
# import io

# # Assuming `readKey` contains your JSON data
# json_data = json.loads(readKey)

# # Create a Spark session
# spark = SparkSession.builder.appName("example").getOrCreate()

# races_schema = StructType(fields = [StructField("raceId", IntegerType(), False),
#                                    StructField("year", IntegerType(), True),
#                                    StructField("round", IntegerType(), True),
#                                    StructField("circuitID", IntegerType(), True),
#                                    StructField("name", StringType(), True),
#                                    StructField("date", DateType(), True),
#                                    StructField("time", StringType(), True),
#                                    StructField("url", StringType(), True)
# ])

# # # Convert the RDD to a DataFrame
# # json_df = spark.read.schema(races_schema).json(json_data)

# json_df = spark.createDataFrame(json_data)

# # Now you can use the DataFrame as needed
# json_df.show()

# print(json_df.count())





# json_data_output = json_df.toJSON().collect()

# json_output_string = "\n".join(json_data_output)



# from pyspark.sql import SparkSession

# spark = SparkSession.builder \
#     .appName("SparkMinioExample") \
#     .config("spark.hadoop.fs.s3a.access.key", "SrojYLRGNPU1JgoC0YHi") \
#     .config("spark.hadoop.fs.s3a.secret.key", "k7zVijm38IqZjTldwvJhl8ACKMWV2cx0g8IdJXF7") \
#     .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000/") \
#     .getOrCreate()

# data = spark.read.json("s3a://browser/raw/customers.json")

# data.show()


from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SparkMinioExample") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://myminio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "True") \
    .getOrCreate()

data = spark.read.csv("s3a://raw/", header=True)

data.show()

data.printSchema()

data.write.csv("s3a://raw/customerscSV", mode="overwrite", header=True)

# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# # Define the SparkSession
# spark = SparkSession.builder \
#     .appName("PythonSparkExample") \
#     .getOrCreate()

# # Define your data as a list of tuples
# data = [(1, "Alice", 28),
#         (2, "Bob", 25),
#         (3, "Carol", 32)]

# # Define the schema for the DataFrame
# schema = StructType([
#     StructField("id", IntegerType(), True),
#     StructField("name", StringType(), True),
#     StructField("age", IntegerType(), True)])

# # Create a DataFrame from the data and schema
# df = spark.createDataFrame(data, schema)

# # Show the contents of the DataFrame
# df.show()

# # Stop the SparkSession
# spark.stop()
