import argparse
from src.etl.silver.build_match_facts import build_match_facts
from src.utils.spark_functions import create_spark_session
from src.utils.logger import get_logger, configure_global_logging

configure_global_logging()
logger = get_logger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run the match fact table transformation."
    )

    parser.add_argument(
        "--base_path",
        type=str,
        default="/home/maria/Desktop/league_analytics/delta",
        help="Path to the base delta data. (default: %(default)s)"
    )


    return parser.parse_args()


def main(base_path: str):
    """
    Main function that runs the match fact etl pipeline.
    """
    logger.info("Starting Spark session...")
    spark = create_spark_session()

    logger.info(f"Executing match fact etl pipeline with:")
    logger.info(f"  base_path = {base_path}")

    # Call your match fact etl function
    build_match_facts(spark, base_path)

    logger.info("Stopping Spark session...")
    spark.stop()
    logger.info("match fact etl of data completed successfully.")


if __name__ == "__main__":
    # Expecting: python script.py <base_path> <bronze_path>
    args = parse_args()

    # Access with args.base_path and args.bronze_path
    main(args.base_path)