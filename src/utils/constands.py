from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DateType, LongType
from datetime import datetime

START_DATE = datetime(2025, 8, 18, 8, 0)
END_DATE = datetime(2025, 8, 18, 8, 0) 
YEARLY_ROUNDS = 38

TEAMS_LIST = ['Chelsea', 'Arsenal', 'Liverpool', 'Manchester City', 'Aston Villa', 'Nottingham', 'Manchester Utd', 'Newcastle', 'Tottenham Hotspur', 'Brighton', 'Crystal Palace', 'Everton', 'West Ham', 'Sunderland', 'Wolves', 'Fulham', 'Burnley', 'Leeds United', 'Brentford', 'Bournemouth']


RAW_SCORES_SCHEMA = StructType([
    StructField("game_id", StringType(), nullable=False),
    StructField("home_goals", StringType(), nullable=False),
    StructField("away_goals", StringType(), nullable=False),
    StructField("ingestion_time", StringType(), nullable=True),
])

RAW_SCHEDULE_SCHEMA = StructType([
    StructField("game_id", StringType(), nullable=False),
    StructField("round", StringType(), nullable=False),
    StructField("home_team", StringType(), nullable=False),
    StructField("away_team", StringType(), nullable=True),
    StructField("game_start_time", StringType(), nullable=True),
])

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

MATCH_SCHEMA = StructType([
    StructField("game_id", StringType(), nullable=True),
    StructField("round", IntegerType(), nullable=True),
    StructField("home_team", StringType(), nullable=True),
    StructField("away_team", StringType(), nullable=True),
    StructField("match_time", TimestampType(), nullable=True),
    StructField("match_date", DateType(), nullable=True),
    StructField("home_goals", IntegerType(), nullable=True),
    StructField("away_goals", IntegerType(), nullable=True),
    StructField("ingestion_time", TimestampType(), nullable=True),
    StructField("ingestion_date", DateType(), nullable=True),
    StructField("goal_difference", IntegerType(), nullable=True),
    StructField("match_outcome", StringType(), nullable=True),
])

STANDIGS_FINAL_SCHEMA = StructType([
    StructField("ranking", IntegerType(), nullable=False),
    StructField("team", StringType(), nullable=True),
    StructField("wins", LongType(), nullable=True),
    StructField("draws", LongType(), nullable=True),
    StructField("losses", LongType(), nullable=True),
    StructField("goals_for", LongType(), nullable=True),
    StructField("goals_against", LongType(), nullable=True),
    StructField("points", LongType(), nullable=True),
])