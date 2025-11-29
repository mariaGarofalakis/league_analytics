# src/silver/build_match_facts.py

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

def build_match_facts(spark: SparkSession, base_path: str):
    # Load bronze Delta tables
    schedule = spark.read.format("delta").load(os.path.join(base_path, "bronze/schedule"))
    scores = spark.read.format("delta").load(os.path.join(base_path, "bronze/scores"))

    # Join them on game_id and enrich
    matches = (
        schedule.join(scores, on="game_id", how="inner")
        .withColumnRenamed("home_team", "home")
        .withColumnRenamed("away_team", "away")
        .withColumnRenamed("game_start_time", "match_date")
        .withColumn("goal_difference", F.col("home_goals") - F.col("away_goals"))
        .withColumn(
            "match_outcome",
            F.when(F.col("home_goals") > F.col("away_goals"), F.lit("home_win"))
             .when(F.col("home_goals") < F.col("away_goals"), F.lit("away_win"))
             .otherwise(F.lit("draw"))
        )
    )

    # Write the silver table
    matches.write.format("delta").mode("overwrite").save(
        os.path.join(base_path, "silver/match_facts")
    )
