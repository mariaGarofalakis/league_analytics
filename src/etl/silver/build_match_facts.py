# src/silver/build_match_facts.py

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime
import os
from src.validators.dataframe_validation import validate_dataframe
from src.utils.constands import MATCH_SCHEMA, YEARLY_ROUNDS
from src.utils.logger import get_logger
from src.validators.runner import ExpectationRunner

logger = get_logger()



def build_match_facts(spark: SparkSession, base_path: str):
    # Load bronze Delta tables
    schedule = spark.read.format("delta").load(os.path.join(base_path, "bronze/schedule"))
    scores = spark.read.format("delta").load(os.path.join(base_path, "bronze/scores"))

    # Join them on game_id and enrich
    matches = (
        schedule.join(scores, on="game_id", how="inner")
        .withColumnRenamed("game_start_time", "match_time")
        .withColumn("match_date", F.to_date("match_time"))
        .withColumn("goal_difference", F.col("home_goals") - F.col("away_goals"))
        .withColumn(
            "match_outcome",
            F.when(F.col("home_goals") > F.col("away_goals"), F.lit("home_win"))
             .when(F.col("home_goals") < F.col("away_goals"), F.lit("away_win"))
             .otherwise(F.lit("draw"))
        )
    )
    logger.info("match facts transformation is ready now validating")

    ########### Validate  match facts #######################
    runner = ExpectationRunner(mode="strict")
    logger.info("Validation schedule dataframe.... ")
    if not validate_dataframe(
        df=matches, 
        runner=runner,
        schema=MATCH_SCHEMA,
        unique_columns=["game_id"],
        not_null_columns=matches.columns, # check all columns
        values_between={
           "round": {"min_value": 1, "max_value": YEARLY_ROUNDS},
           "home_goals": {"min_value": 0},
           "away_goals": {"min_value": 0},
           
        },
        team_unique_per_round=["home_team", "away_team"],
        diff_columns=[["home_team","away_team"]],
        date_time_in_range={
            "match_date": {"start_date": datetime(2025, 8, 22), "end_date": datetime(2026, 5, 18)},
            "match_time": {"start_date": datetime(2025, 8, 22), "end_date": datetime(2026, 5, 18)},
            "ingestion_time": {"start_date": datetime(2025, 8, 22), "end_date": datetime(2026, 5, 18)},
            "ingestion_date": {"start_date": datetime(2025, 8, 22), "end_date": datetime(2026, 5, 18)},
            },
        game_ids_match=[schedule, scores, matches]
        
    ):
        logger.error("🚫 fact matches failed validation.")
        return
    
    

    logger.info(f"✅ Match Facts table transformation completed ready to save to delt in path {base_path}/silver")

    # Write the silver table
    matches.write.format("delta").mode("overwrite").save(
        os.path.join(base_path, "silver/match_facts")
    )
