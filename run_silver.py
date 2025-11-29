# run_ingest.py
from pyspark.sql import SparkSession
from src.silver.build_match_facts import build_match_facts
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
path = "/home/maria/Desktop/league_analytics/delta"

# Run the ingest function
logger.info("Executing ingestion pipline")
build_match_facts(spark, path)

spark.stop()
