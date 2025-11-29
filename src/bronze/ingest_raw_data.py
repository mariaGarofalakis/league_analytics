from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import ast, re
from datetime import datetime
from src.utils.logger import get_logger
from src.validators.dataframe_validation import validate_dataframe
import os
from src.validators.runner import ExpectationRunner
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from src.validators.expectations import Expectations

logger = get_logger(__name__)



SCORES_SCHEMA = StructType([
    StructField("game_id", StringType(), nullable=False),
    StructField("home_goals", IntegerType(), nullable=False),
    StructField("away_goals", IntegerType(), nullable=False),
    StructField("ingestion_time", TimestampType(), nullable=True),
])

SCHEDULE_SCHEMA = StructType([
    StructField("game_id", StringType(), nullable=False),
    StructField("round", IntegerType(), nullable=False),
    StructField("home_team", StringType(), nullable=False),
    StructField("away_team", StringType(), nullable=True),
    StructField("game_start_time", TimestampType(), nullable=True),
])

def drop_mismatched_game_ids(df1, df2, result, df1_name="scores_df", df2_name="schedule_df"):
    

    msg = result.message

    df1_ids = re.search(r"missing in df1: (\[.*?\])", msg)
    df2_ids = re.search(r"missing in df2: (\[.*?\])", msg)

    drop_from_df2 = ast.literal_eval(df1_ids.group(1)) if df1_ids else []
    drop_from_df1 = ast.literal_eval(df2_ids.group(1)) if df2_ids else []

    if drop_from_df1:
        df1 = df1.filter(~F.col("game_id").isin(drop_from_df1))
    if drop_from_df2:
        df2 = df2.filter(~F.col("game_id").isin(drop_from_df2))

    return df1, df2



def ingest_raw_data(spark: SparkSession, input_path: str, output_path: str):
    logger.info("Starting bronze layer ingestion...")

    
    logger.info(f"Reading source data")
    scores_df = spark.read.csv(input_path+"/scores.csv", header=True)
    schedule_df = spark.read.csv(input_path+"/schedule.csv", header=True)

    ########### Validation of source data #######################
    # Validating source dataframes (mode=soft)
    # in order not to fail before transformations
    runner = ExpectationRunner(mode="soft")
    logger.info("Validation scores dataframe.... ")
    if not validate_dataframe(
        df=scores_df, 
        runner=runner,
    #    schema=SCORES_SCHEMA,
        unique_columns=["game_id"],
        not_null_columns=scores_df.columns, # check all columns
        values_between={
           "home_goals": {"min_value": 0},
           "away_goals": {"min_value": 0}
        },
        date_time_in_range={
           "ingestion_time": {"start_date": datetime(2025, 8, 22), "end_date": datetime(2026,5,18)}   
        }

        ):
            logger.error("🚫 scores.csv failed validation.")
            
    
    runner = ExpectationRunner(mode="soft")
    logger.info("Validation schedule dataframe.... ")
    if not validate_dataframe(
        df=schedule_df, 
        runner=runner,
        unique_columns=["game_id"],
        not_null_columns=schedule_df.columns, # check all columns
        values_between={
           "round": {"min_value": 1, "max_value": 38},
           
        },
        date_time_in_range={
           "game_start_time": {"start_date": datetime(2025, 8, 22), "end_date": datetime(2026,5,18)}   
        },
        team_unique_per_round=["home_team", "away_team"],
        diff_columns=[["home_team","away_team"]]

        ):
            logger.error("🚫 schedule.csv failed validation.")
            

    logger.info("Source validation finished. Transforming and writing to Delta...")

    ########### Transformation of source data #######################

    # Remove null values and dublicate raws
    scores_df = scores_df.dropDuplicates().dropna()
    schedule_df = schedule_df.dropDuplicates().dropna()

    # Cast columns 
    scores_df = (
        scores_df
        .withColumn("game_id", F.col("game_id"))
        .withColumn("home_goals", F.col("home_goals").cast("double").cast("int"))
        .withColumn("away_goals", F.col("away_goals").cast("double").cast("int"))
        .withColumn("ingestion_time", F.col("ingestion_time").cast("timestamp"))
    )

    schedule_df = (
        schedule_df
        .withColumn("game_id", F.col("game_id"))
        .withColumn("round", F.col("round").cast("double").cast("int"))
        .withColumn("home_team", F.col("home_team"))
        .withColumn("away_team", F.col("away_team"))
        .withColumn("game_start_time", F.col("game_start_time").cast("timestamp"))
    )

    # create ingestion_date column for partitioning
    scores_df = scores_df.withColumn("ingestion_date", F.to_date("ingestion_time"))
    # Check if both dataframes have same game_id and if not remove
    # the raws that differantiate
    ex = Expectations()
    are_same, result = ex.game_ids_match(scores_df, schedule_df)
    if not are_same:
        scores_df, schedule_df = drop_mismatched_game_ids(scores_df, schedule_df, result, "scores_df", "schedule_df")
    
    # check if both dataframes have same number of rows

    if not scores_df.count() == schedule_df.count():
         logger.error("The scores and schedule dataframe does not have the same number of raws")
         return
    else:
         logger.info("The scores and schedule dataframe does not have the same number of raws! nontinue with further validation")

    

    ########### Validation of bronze data #######################

    runner = ExpectationRunner(mode="strict")
    logger.info("Validation scores dataframe.... ")
    if not validate_dataframe(
        df=scores_df, 
        runner=runner,
        schema=SCORES_SCHEMA,
        unique_columns=["game_id"],
        not_null_columns=scores_df.columns, # check all columns
        values_between={
           "home_goals": {"min_value": 0},
           "away_goals": {"min_value": 0}
        },
        date_time_in_range={
           "ingestion_time": {"start_date": datetime(2025, 8, 22), "end_date": datetime(2026,5,18)}   
        }

        ):
            logger.error("🚫 scores.csv failed validation.")
            return
            
            
    
    runner = ExpectationRunner(mode="strict")
    logger.info("Validation schedule dataframe.... ")
    if not validate_dataframe(
        df=schedule_df, 
        runner=runner,
        schema=SCHEDULE_SCHEMA,
        unique_columns=["game_id"],
        not_null_columns=schedule_df.columns, # check all columns
        values_between={
           "round": {"min_value": 1, "max_value": 38},
           
        },
        date_time_in_range={
           "game_start_time": {"start_date": datetime(2025, 8, 22), "end_date": datetime(2026,5,18)}   
        },
        team_unique_per_round=["home_team", "away_team"],
        diff_columns=[["home_team","away_team"]]

        ):
            logger.error("🚫 schedule.csv failed validation.")
            return
    
    logger.info("✅ The bronze validation finished successfully ready to save to delta")


    
    schedule_df.write.format("delta").mode("overwrite").save(
        os.path.join(output_path, "bronze/schedule")
    )
    scores_df.write.format("delta").partitionBy("ingestion_date").mode("overwrite").save(
        os.path.join(output_path, "bronze/scores")
    )

    logger.info("✅ Bronze ingestion completed and data written to Delta.")
