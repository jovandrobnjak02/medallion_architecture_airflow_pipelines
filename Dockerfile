FROM apache/airflow:2.10.5

USER root

RUN apt update && \
    apt-get install -y openjdk-17-jdk ant wget && \
    apt-get clean

ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64/
RUN export JAVA_HOME

USER airflow

RUN pip install --no-cache-dir \
    apache-airflow \
    apache-airflow-providers-postgres \
    apache-airflow-providers-apache-spark \
    grpcio-status \
    pyspark \
    psycopg2-binary \
    boto3 \
    s3fs \
    minio \
    fastparquet