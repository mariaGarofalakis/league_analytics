# run_ingest.py
from pyspark.sql import SparkSession
from src.bronze.ingest_raw_data import ingest_raw_data
from src.utils.logger import configure_global_logging, get_logger
from delta import configure_spark_with_delta_pip


configure_global_logging()
logger = get_logger()



builder = (
    SparkSession.builder
    .appName("LeagueAnalytics")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Define paths
input_path = "/home/maria/Desktop/league_analytics/data"
output_path = "/home/maria/Desktop/league_analytics/delta"

# Run the ingest function
logger.info("Executing ingestion pipline")
ingest_raw_data(spark, input_path, output_path)

spark.stop()
