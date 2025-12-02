import argparse
from src.etl.gold.compute_standings import compute_standings
from src.utils.spark_functions import create_spark_session
from src.utils.logger import get_logger, configure_global_logging



def parse_args():
    parser = argparse.ArgumentParser(
        description="Run the total standings table transformation."
    )

    parser.add_argument(
        "--base_path",
        type=str,
        default="delta",
        help="Path to the base delta data. (default: %(default)s)"
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


def main(base_path: str, week_itr: int,  batch: bool, log_path):
    """
    Main function that runs the total standings etl pipeline.
    """
    configure_global_logging(log_path)
    logger = get_logger(__name__)
    logger.info("Starting Spark session...")
    spark = create_spark_session()

    logger.info(f"Executing total standings etl pipeline with:")
    logger.info(f"  base_path = {base_path}")

    # Call your total standings etl function
    compute_standings(spark, base_path, week_itr, batch)

    logger.info("Stopping Spark session...")
    spark.stop()
    logger.info("total standings etl of data completed successfully.")


if __name__ == "__main__":
    # Expecting: python script.py <base_path> <bronze_path>
    args = parse_args()

    # Access with args.base_path and args.bronze_path
    main(args.base_path, args.week_itr, args.batch, args.log_path)