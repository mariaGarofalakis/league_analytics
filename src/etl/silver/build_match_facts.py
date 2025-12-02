# src/silver/build_match_facts.py

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
from src.validators.dataframe_validation import validate_dataframe
from src.utils.constands import MATCH_SCHEMA
from src.utils.logger import get_logger
from src.validators.runner import ExpectationRunner
from src.utils.spark_functions import get_week_info

logger = get_logger()



def build_match_facts(spark: SparkSession, base_path: str, week_itr: int, batch: bool):
    # Load bronze Delta tables

    start_time, end_time, min_round, max_round, updated_at = get_week_info(week_itr=week_itr, batch=batch)

    if batch:
        scedule_path = "bronze/schedule_hist"
        scores_path = "bronze/scores_hist"
    else:
        scedule_path = "bronze/schedule"
        scores_path = "bronze/scores"

    schedule = spark.read.format("delta").load(os.path.join(base_path, scedule_path)).filter(F.col("updated_at")==updated_at).drop("updated_at")
    scores = spark.read.format("delta").load(os.path.join(base_path, scores_path)).filter(F.col("updated_at")==updated_at)

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
           "round": {"min_value": min_round, "max_value": max_round},
           "home_goals": {"min_value": 0},
           "away_goals": {"min_value": 0},
           
        },
        team_unique_per_round=["home_team", "away_team"],
        diff_columns=[["home_team","away_team"]],
        date_time_in_range={
            "match_time": {"start_date": start_time, "end_date": end_time},
            "ingestion_time": {"start_date": start_time, "end_date": end_time},
            "updated_at": {"start_date": updated_at, "end_date": updated_at}
            },
        game_ids_match=[schedule, scores, matches]
        
    ):
        logger.error("🚫 fact matches failed validation.")
        return
    
    

    logger.info(f"✅ Match Facts table transformation completed ready to save to delt in path {base_path}/silver")

    # Write the silver table
    if batch:
        matches.write.format("delta").partitionBy("updated_at").mode("append").save(
        os.path.join(base_path, "silver/match_facts_hist")
    )
    else:
        matches.write.format("delta").mode("overwrite").save(
        os.path.join(base_path, "silver/match_facts")
    )

    
