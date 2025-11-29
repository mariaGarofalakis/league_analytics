# docker/Dockerfile
FROM apache/airflow:2.8.0-python3.9

USER root
# Install Spark dependencies
RUN apt-get update && apt-get install -y curl openjdk-11-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install Spark manually
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
RUN curl -L https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    | tar -xz -C /opt/ && mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} $SPARK_HOME

ENV PATH=$PATH:$SPARK_HOME/bin

# Delta Lake support
ENV DELTA_VERSION=3.0.0
ENV PYSPARK_SUBMIT_ARGS="--packages io.delta:delta-core_2.12:${DELTA_VERSION} --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog pyspark-shell"

# Copy requirements
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

USER airflow
