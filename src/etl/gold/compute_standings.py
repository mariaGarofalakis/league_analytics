from pyspark.sql import SparkSession, functions as F, Window
import os
from src.utils.logger import get_logger
from src.validators.runner import ExpectationRunner
from src.validators.dataframe_validation import validate_dataframe
from src.utils.spark_functions import get_week_info
from src.utils.constands import TEAMS_LIST, STANDIGS_FINAL_SCHEMA

logger = get_logger(__name__)




def compute_standings(spark: SparkSession, base_path: str, week_itr: int, batch: bool):
    logger.info("Starting the calculation of the standins_final table.....")
    # Load match facts
    if batch:
        match_path = "silver/match_facts_hist"
    else:
        match_path = "silver/match_facts"


    start_time, end_time, min_round, max_round, updated_at = get_week_info(week_itr=week_itr, batch=batch)
    logger.info("1. Loading match facts table....")
    matches = spark.read.format("delta").load(os.path.join(base_path, match_path))#.filter(F.col("updated_at")==updated_at)

    logger.info("2. Calculating home team stats....")
    # HOME TEAM STATS
    home_stats = matches.select(
        F.col("home_team").alias("team"),
        F.when(F.col("match_outcome") == "home_win", 1).otherwise(0).alias("wins"),
        F.when(F.col("match_outcome") == "draw", 1).otherwise(0).alias("draws"),
        F.when(F.col("match_outcome") == "away_win", 1).otherwise(0).alias("losses"),
        F.col("home_goals").alias("goals_for"),
        F.col("away_goals").alias("goals_against")
    )

    logger.info("3. Calculating home team stats....")
    # AWAY TEAM STATS
    away_stats = matches.select(
        F.col("away_team").alias("team"),
        F.when(F.col("match_outcome") == "away_win", 1).otherwise(0).alias("wins"),
        F.when(F.col("match_outcome") == "draw", 1).otherwise(0).alias("draws"),
        F.when(F.col("match_outcome") == "home_win", 1).otherwise(0).alias("losses"),
        F.col("away_goals").alias("goals_for"),
        F.col("home_goals").alias("goals_against")
    )

    # Combine home + away stats
    logger.info("4. Combine home + away stats....")
    all_stats = home_stats.unionByName(away_stats)

    # Aggregate by team
    logger.info("5. Agregate for each team....")
    standings = all_stats.groupBy("team").agg(
        F.sum("wins").alias("wins"),
        F.sum("draws").alias("draws"),
        F.sum("losses").alias("losses"),
        F.sum("goals_for").alias("goals_for"),
        F.sum("goals_against").alias("goals_against"),
        (F.sum("wins") * 3 + F.sum("draws")).alias("points")
    )

    # Add goal difference column
    standings = standings.withColumn(
        "goal_difference",
        F.col("goals_for") - F.col("goals_against")
    )

    # Since the pipeline does not ecaptulates exactlly every round we have to
    # add the teams that are not included in the specific batch
    # Convert expected list to a Spark DF
    expected_df = spark.createDataFrame([(t,) for t in TEAMS_LIST], ["team"])

    # Identify missing teams using left_anti join
    missing_df = expected_df.join(standings.select("team").distinct(), "team", "left_anti")

    # Build zero-filled rows for all non-team columns
    zero_exprs = [
        F.lit(0).alias(c)
        for c in standings.columns
        if c != "team"
    ]

    missing_filled = missing_df.select(
        "team",
        *zero_exprs
    )

    # Append results
    standings = standings.unionByName(missing_filled)


    # Window for ranking
    ranking_window = Window.orderBy(
        F.col("points").desc(),
        F.col("goal_difference").desc(),
        F.col("goals_for").desc(),
        F.col("team").asc()        
    )

    # Assign rank
    logger.info("6. Assigning ranking....")
    standings = standings.withColumn("ranking", F.row_number().over(ranking_window))

    

    
    # Final column order
    standings_final = standings.withColumn("updated_at", F.lit(updated_at)).select(
        "ranking", "team", "wins", "draws", "losses",
        "goals_for", "goals_against", "points", "updated_at"
    ).orderBy("ranking")

    logger.info("The calculation of standins_final is done start validating.....")
    ########### Validate  match facts #######################
    runner = ExpectationRunner(mode="strict")
    logger.info("Validation schedule dataframe.... ")
    if not validate_dataframe(
        df=standings_final, 
        runner=runner,
        schema=STANDIGS_FINAL_SCHEMA,
        unique_columns=["team"],
        not_null_columns=standings_final.columns, # check all columns
        values_between={
           "ranking": {"min_value": 1, "max_value": len(TEAMS_LIST)},
           "wins": {"min_value": 0, "max_value":max_round},
           "draws": {"min_value": 0, "max_value":max_round},
           "losses": {"min_value": 0, "max_value":max_round},
           
        },
        column_has_values={
            "team" : TEAMS_LIST
        }

    ):
        logger.error("🚫 standins_final failed validation.")
        return
    

    logger.info("✅ standins_final validation finished successfully, saving table....")

    # Save to Gold
    if batch:
        standings_final.write.format("delta").partitionBy("updated_at").mode("append").save(
        os.path.join(base_path, "gold/standings_hist")
    )
    else:
        standings_final.drop("updated_at").write.format("delta").mode("overwrite").save(
        os.path.join(base_path, "gold/standings")
    )
    

    print("✅ Standings successfully written to gold/standings")
