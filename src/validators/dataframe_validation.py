# src/validators/bronze_validator.py

from src.validators.expectations import Expectations
from src.validators.runner import ExpectationRunner
from src.utils.logger import get_logger
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from typing import List, Dict, Optional
from itertools import combinations

logger = get_logger(__name__)

def validate_dataframe(
    df: DataFrame,
    runner: Optional["ExpectationRunner"] = None,
    schema: Optional[StructType] = None,
    unique_columns: Optional[List[str]] = None,
    not_null_columns: Optional[List[str]] = None,
    values_between: Optional[dict] = None,
    date_time_in_range: Optional[dict] = None,
    team_unique_per_round: Optional[List[str]]= None,
    diff_columns: Optional[List[List[str]]]=None,
    game_ids_match: Optional[List[DataFrame]]=None,
    column_has_values: Optional[dict] = None
)  -> bool:
    logger.info("🔎 Validating DataFrame...")
    
    exp = Expectations()

    if schema:
        runner.run(exp.schema(df, schema), "Schema validation")
    if diff_columns is not None:
        for columns in diff_columns:
            runner.run(exp.diff_columns(df, columns[0],columns[1]), f"{columns[0]} and {columns[1]} same")
    if unique_columns is not None:
        for cl in unique_columns:
            runner.run(exp.column_unique(df, cl), f"{cl} uniqueness")
    if not_null_columns is not None:
        for cl in not_null_columns:
            runner.run(exp.column_not_null(df, cl), f"{cl} null check")
    if team_unique_per_round is not None:
        for cl in team_unique_per_round:
            runner.run(exp.team_unique_per_round(df, cl), f"{cl} teams unique on same round")
    if values_between is not None:
        for col, bounds in values_between.items():
            min_val = bounds["min_value"] if "min_value" in bounds else None
            max_val = bounds["max_value"] if "max_value" in bounds else None

            runner.run(
                exp.column_values_between(df, col, min_val=min_val, max_val=max_val),
                f"Range check on '{col}'"
            )
    if column_has_values:
        for col, list in column_has_values.items():
            runner.run(
                exp.column_has_values(df, col, list),
                f"Allowed values on '{col}'"
            )
    if date_time_in_range is not None:
        for col, bounds in date_time_in_range.items():
            start_date = bounds["start_date"] if "start_date" in bounds else None
            end_date = bounds["end_date"] if "end_date" in bounds else None

            runner.run(
                exp.column_datetime_in_range(df, col, start_date=start_date, end_date=end_date),
                f"Date time range check on '{col}'"
            )
    if game_ids_match:
        for i, j in combinations(range(len(game_ids_match)), 2):
            df1, df2 = game_ids_match[i], game_ids_match[j]
            label = f"df[{i}] vs df[{j}]"
            runner.run(exp.game_ids_match(df1, df2), f"check {label} matching of game_id columns")

    runner.report()

    if runner.has_errors():
        logger.error("Failed validation.")
        return False

    logger.info("✅ Validation passed.")
    return True
