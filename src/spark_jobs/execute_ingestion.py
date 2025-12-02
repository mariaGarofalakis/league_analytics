import argparse
from src.etl.bronze.ingest_raw_data import ingest_raw_data
from src.utils.spark_functions import create_spark_session
from src.utils.logger import get_logger, configure_global_logging



def parse_args():
    parser = argparse.ArgumentParser(
        description="Run the league analytics ingestion pipeline."
    )

    parser.add_argument(
        "--source_path",
        type=str,
        default="data",
        help="Path to the raw source data. (default: %(default)s)"
    )

    parser.add_argument(
        "--bronze_path",
        type=str,
        default="delta",
        help="Path where bronze Delta tables will be stored. (default: %(default)s)"
    )

    parser.add_argument(
        "--week_itr",
        type=int,
        default=0,
        help="The number of week iteration 0-39. (default: %(default)s)"
    )

    parser.add_argument(
        "--batch",
        type=bool,
        default=False,
        help="True if we batch over a week, False if we calculate the total total_standing table. (default: %(default)s)"
    )

    parser.add_argument(
        "--log_path",
        type=str,
        default="logs/sandings_historical.log",
        help="The logger path"
    )

    return parser.parse_args()


def main(source_path: str, bronze_path: str, week_itr: int, batch: bool, log_path):
    """
    Main function that runs the ingestion pipeline.
    """
    configure_global_logging(log_path)
    logger = get_logger(__name__)
    logger.info("Starting Spark session...")
    spark = create_spark_session()

    logger.info(f"Executing ingestion pipeline with:")
    logger.info(f"  source_path = {source_path}")
    logger.info(f"  bronze_path = {bronze_path}")

    # Call your ingestion function
    ingest_raw_data(spark, source_path, bronze_path, week_itr, batch)

    logger.info("Stopping Spark session...")
    spark.stop()
    logger.info("Ingestion of source data completed successfully.")


if __name__ == "__main__":
    # Expecting: python script.py <source_path> <bronze_path>
    args = parse_args()

    # Access with args.source_path and args.bronze_path
    main(args.source_path, args.bronze_path, args.week_itr, args.batch, args.log_path)