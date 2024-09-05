FROM jupyter/pyspark-notebook:latest

USER root

COPY hadoop-aws-3.3.4.jar /usr/local/spark/jars
COPY aws-java-sdk-bundle-1.12.262.jar /usr/local/spark/jars
COPY postgresql-42.2.5.jar /usr/local/spark/jars
COPY mysql-connector-java-8.0.28.jar /usr/local/spark/jars