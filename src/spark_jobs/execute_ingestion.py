import argparse
from src.bronze.ingest_raw_data import ingest_raw_data
from src.utils.spark_functions import create_spark_session
from src.utils.logger import get_logger, configure_global_logging

configure_global_logging()
logger = get_logger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run the league analytics ingestion pipeline."
    )

    parser.add_argument(
        "--source_path",
        type=str,
        default="/home/maria/Desktop/league_analytics/data",
        help="Path to the raw source data. (default: %(default)s)"
    )

    parser.add_argument(
        "--bronze_path",
        type=str,
        default="/home/maria/Desktop/league_analytics/delta",
        help="Path where bronze Delta tables will be stored. (default: %(default)s)"
    )

    return parser.parse_args()


def main(source_path: str, bronze_path: str):
    """
    Main function that runs the ingestion pipeline.
    """
    logger.info("Starting Spark session...")
    spark = create_spark_session()

    logger.info(f"Executing ingestion pipeline with:")
    logger.info(f"  source_path = {source_path}")
    logger.info(f"  bronze_path = {bronze_path}")

    # Call your ingestion function
    ingest_raw_data(spark, source_path, bronze_path)

    logger.info("Stopping Spark session...")
    spark.stop()
    logger.info("Ingestion of source data completed successfully.")


if __name__ == "__main__":
    # Expecting: python script.py <source_path> <bronze_path>
    args = parse_args()

    # Access with args.source_path and args.bronze_path
    main(args.source_path, args.bronze_path)