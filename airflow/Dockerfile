# Use the latest Python 3.8 version of Apache Airflow as base image
FROM apache/airflow:latest-python3.11

# Switch to root user for setup
USER root

# Update package repository and install necessary packages
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        default-jdk \
        ant \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/default-java
RUN export JAVA_HOME

USER airflow

# Set environment variable for Airflow home directory
ARG AIRFLOW_HOME=/opt/airflow

# Copy DAGs into the Airflow DAGs directory
COPY dags ${AIRFLOW_HOME}/dags

# Install or upgrade pip
RUN pip install --upgrade pip

# Install boto3 (example package)
RUN pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org boto3

RUN pip install pyspark==3.5.0

RUN pip install selenium webdriver-manager beautifulsoup4 minio lxml pandas

# Install additional Python packages
RUN pip install yfinance confluent_kafka minio pandas

RUN pip install apache-airflow-providers-apache-spark

RUN pip install apache-airflow-providers-amazon

COPY hadoop-aws-3.3.4.jar /home/airflow/.local/lib/python3.11/site-packages/pyspark/jars
COPY aws-java-sdk-bundle-1.12.262.jar /home/airflow/.local/lib/python3.11/site-packages/pyspark/jars
COPY postgresql-42.2.5.jar /home/airflow/.local/lib/python3.11/site-packages/pyspark/jars
COPY mysql-connector-java-8.0.28.jar /home/airflow/.local/lib/python3.11/site-packages/pyspark/jars

# Switch back to the airflow user
USER airflow

# # Set the entrypoint to airflow-entrypoint.sh script
# ENTRYPOINT ["/opt/airflow/scripts/airflow-entrypoint.sh"]

# Optionally, define default command (e.g., airflow webserver)
CMD ["webserver"]
