import argparse
from src.etl.bronze.ingest_raw_data import ingest_raw_data
from src.utils.spark_functions import create_spark_session
from src.utils.logger import get_logger, configure_global_logging




def parse_args():
    parser = argparse.ArgumentParser(
        description="Job that just saves file from gold delta to csv."
    )

    parser.add_argument(
        "--input_file_path",
        type=str,
        default="standings",
        help="Path to the  data. (default: %(default)s)"
    )

    parser.add_argument(
        "--output_file_name",
        type=str,
        default="standings_final.csv",
        help="output file name"
    )

    parser.add_argument(
        "--log_path",
        type=str,
        default="logs/sandings_historical.log",
        help="The logger path"
    )

    return parser.parse_args()


def main(input_file_path: str, output_file_name: str, log_path):
    """
    Main function that runs the ingestion pipeline.
    """
    configure_global_logging(log_path)
    logger = get_logger(__name__)
    logger.info("Starting Spark session...")
    spark = create_spark_session()
    input_path = "delta/gold"
    output_path = "output"
    logger.info("Loading Delta table")
    delta_table = spark.read.format("delta").load(f"{input_path}/{input_file_path}")
    logger.info(f"Transforming delta table {input_file_path} into csv {output_file_name}")
    delta_table.toPandas().to_csv(f"{output_path}/{output_file_name}",index=False)


if __name__ == "__main__":

    args = parse_args()
    main(args.input_file_path, args.output_file_name, args.log_path)