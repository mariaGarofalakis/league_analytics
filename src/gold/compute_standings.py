# src/gold/compute_standings.py
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import os
import logging

def compute_standings(spark: SparkSession, base_path: str, output_csv_path: str):
    logger = logging.getLogger("final_standings")
    logging.basicConfig(filename='logs/final_standings.log', level=logging.INFO)

    df = spark.read.format("delta").load(os.path.join(base_path, "silver/match_facts"))

    # Explode matches into team-based rows
    home_df = df.selectExpr(
        "game_id",
        "match_date",
        "home as team",
        "home_goals as goals_for",
        "away_goals as goals_against",
        """
        CASE
          WHEN home_goals > away_goals THEN 'win'
          WHEN home_goals = away_goals THEN 'draw'
          ELSE 'loss'
        END as result
        """
    )

    away_df = df.selectExpr(
        "game_id",
        "match_date",
        "away as team",
        "away_goals as goals_for",
        "home_goals as goals_against",
        """
        CASE
          WHEN away_goals > home_goals THEN 'win'
          WHEN away_goals = home_goals THEN 'draw'
          ELSE 'loss'
        END as result
        """
    )

    exploded = home_df.union(away_df)

    agg = exploded.groupBy("team").agg(
        F.sum(F.expr("result = 'win'")).alias("wins"),
        F.sum(F.expr("result = 'draw'")).alias("draws"),
        F.sum(F.expr("result = 'loss'")).alias("losses"),
        F.sum("goals_for").alias("goals_for"),
        F.sum("goals_against").alias("goals_against"),
        (F.sum(F.expr("result = 'win'")) * 3 + F.sum(F.expr("result = 'draw'"))).alias("points")
    )

    # Ranking
    window = Window.orderBy(
        F.desc("points"),
        F.desc("goals_for" - "goals_against"),
        F.desc("goals_for"),
        F.asc("team")
    )

    final = agg.withColumn("ranking", F.row_number().over(window)).select(
        "ranking", "team", "wins", "draws", "losses", "goals_for", "goals_against", "points"
    )

    final.write.option("header", True).mode("overwrite").csv(output_csv_path)

    logger.info("Final standings successfully computed and written to CSV.")
