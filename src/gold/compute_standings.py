from pyspark.sql import SparkSession, functions as F
import os

def compute_standings(spark: SparkSession, base_path: str):
    # Load match facts
    matches = spark.read.format("delta").load(os.path.join(base_path, "silver/match_facts"))

    # === HOME STATS ===
    home_stats = matches.select(
        F.col("home_team").alias("team"),
        F.when(F.col("match_outcome") == "home_win", 1).otherwise(0).alias("wins"),
        F.when(F.col("match_outcome") == "draw", 1).otherwise(0).alias("draws"),
        F.when(F.col("match_outcome") == "away_win", 1).otherwise(0).alias("losses"),
        F.col("home_goals").alias("goals_for"),
        F.col("away_goals").alias("goals_against")
    )

    # === AWAY STATS ===
    away_stats = matches.select(
        F.col("away_team").alias("team"),
        F.when(F.col("match_outcome") == "away_win", 1).otherwise(0).alias("wins"),
        F.when(F.col("match_outcome") == "draw", 1).otherwise(0).alias("draws"),
        F.when(F.col("match_outcome") == "home_win", 1).otherwise(0).alias("losses"),
        F.col("away_goals").alias("goals_for"),
        F.col("home_goals").alias("goals_against")
    )

    # === Combine both home and away stats ===
    all_stats = home_stats.unionByName(away_stats)

    # === Aggregate by team ===
    standings = all_stats.groupBy("team").agg(
        F.sum("wins").alias("wins"),
        F.sum("draws").alias("draws"),
        F.sum("losses").alias("losses"),
        F.sum("goals_for").alias("goals_for"),
        F.sum("goals_against").alias("goals_against"),
        (F.sum("wins") * 3 + F.sum("draws")).alias("points")
    )

    # === Order and rank ===
    standings = standings.withColumn(
        "goal_difference", F.col("goals_for") - F.col("goals_against")
    ).withColumn(
        "ranking", F.row_number().over(
            F.window.OrderBy(
                F.col("points").desc(),
                F.col("goal_difference").desc(),
                F.col("goals_for").desc()
            )
        )
    )

    # === Reorder columns ===
    final_cols = ["ranking", "team", "wins", "draws", "losses", "goals_for", "goals_against", "points"]
    standings_final = standings.select(*final_cols).orderBy("ranking")

    # === Save to gold layer ===
    standings_final.write.format("delta").mode("overwrite").save(os.path.join(base_path, "gold/standings"))

    print("✅ Standings table written to gold/standings")
