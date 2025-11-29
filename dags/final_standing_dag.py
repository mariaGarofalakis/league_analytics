from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from src.bronze.ingest_raw_data import ingest_raw_data
from src.silver.build_match_facts import build_match_facts
from src.gold.compute_standings import compute_standings

import sys
import os
from pyspark.sql import SparkSession

sys.path.append(os.path.abspath("src"))

def get_spark():
    return SparkSession.builder \
        .appName("league_analytics") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

default_args = {
    "start_date": datetime(2025, 8, 25),
}

dag = DAG("final_standings", schedule_interval=None, default_args=default_args, catchup=False)

def run_ingest():
    spark = get_spark()
    ingest_raw_data(spark, "data", "delta")

def run_silver():
    spark = get_spark()
    build_match_facts(spark, "delta")

def run_gold():
    spark = get_spark()
    compute_standings(spark, "delta", "output/standings_final.csv")

t1 = PythonOperator(task_id="ingest_raw", python_callable=run_ingest, dag=dag)
t2 = PythonOperator(task_id="build_match_facts", python_callable=run_silver, dag=dag)
t3 = PythonOperator(task_id="compute_standings", python_callable=run_gold, dag=dag)

t1 >> t2 >> t3
