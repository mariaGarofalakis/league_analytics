from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from src.utils.logger import get_logger

from src.utils.constands import START_DATE, END_DATE, YEARLY_ROUNDS

logger = get_logger(__name__)

def create_spark_session():
    """
    Creates and returns a Spark session with Delta Lake support.
    """
    builder = (
        SparkSession.builder
        .appName("LeagueAnalytics")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    return configure_spark_with_delta_pip(builder).getOrCreate()

def get_weekly_run_datetime(iteration: int) -> datetime:
    """
    Given an iteration number (0–38), return the Monday 08:00 datetime
    corresponding to that iteration.
    """
    if iteration < 0:
        raise ValueError("Iteration must be >= 0")
    
    return START_DATE + timedelta(weeks=iteration) 

def get_week_info(week_itr, batch = False):
    if batch:
        end_time = get_weekly_run_datetime(week_itr+1)
        start_time = get_weekly_run_datetime(week_itr)
        updated_at = end_time
        
        if week_itr == 0:
            
            min_round = week_itr +1
            max_round = week_itr +1
        else:
             min_round = week_itr
             max_round = week_itr +1
    else:
        logger.info("We calculate total total_standings no batching needed...")
        updated_at = END_DATE
        start_time = START_DATE
        end_time = END_DATE
        min_round = 1
        max_round = YEARLY_ROUNDS

    return start_time, end_time, min_round, max_round, updated_at