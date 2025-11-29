from pyspark.sql import DataFrame
from pyspark.sql.functions import col, expr, count, to_timestamp
from pyspark.sql.utils import AnalysisException
from typing import Optional, Tuple
from datetime import datetime
from dataclasses import dataclass
from pyspark.sql.types import StructType, FloatType, StringType, IntegerType, TimestampType



@dataclass
class ExpectationResult:
    message: str
    invalid_df: Optional[DataFrame] = None


class Expectations:

    def schema(self, df: DataFrame, expected_schema: StructType) -> Tuple[bool, ExpectationResult]:
        df_cols = set(df.columns)
        expected_cols = set(field.name for field in expected_schema)
        missing = expected_cols - df_cols
        if missing:
            return False, ExpectationResult(f"Missing columns: {sorted(missing)}")

        mismatches = []
        df_schema = {field.name: field.dataType.simpleString() for field in df.schema}
        for field in expected_schema.fields:
            actual = df_schema.get(field.name)
            expected = field.dataType.simpleString()
            if actual != expected:
                mismatches.append(f"{field.name}: expected {expected}, got {actual}")

        if mismatches:
            return False, ExpectationResult(f"Column type mismatches: {mismatches}")

        return True, ExpectationResult("Schema is valid")

    def column_not_null(self, df: DataFrame, column: str) -> Tuple[bool, ExpectationResult]:
        nulls = df.filter(col(column).isNull()).count()
        if nulls > 0:
            return False, ExpectationResult(f"Column '{column}' has {nulls} nulls")
        return True, ExpectationResult(f"Column '{column}' has no nulls")

    def column_unique(self, df: DataFrame, column: str) -> Tuple[bool, ExpectationResult]:
        total = df.count()
        distinct = df.select(column).distinct().count()
        if total != distinct:
            return False, ExpectationResult(f"Column '{column}' has {total - distinct} duplicate values")
        return True, ExpectationResult(f"Column '{column}' is unique")

    def column_values_between(
        self,
        df: DataFrame,
        column: str,
        min_val: Optional[float] = None,
        max_val: Optional[float] = None
    ) -> Tuple[bool, ExpectationResult]:
        try:
            df_casted = df.withColumn(column,col(column).cast(StringType())) \
                .withColumn(column,col(column).cast(FloatType())) 
           
            if min_val:
                min_val = float(min_val)
            if max_val:
                max_val = float(max_val)
            condition = col(column).isNull()
            if min_val is not None and max_val is not None:
                condition |= (col(column) < min_val) | (col(column) > max_val)
                range_msg = f"outside range [{min_val}, {max_val}]"
            elif min_val is not None:
                condition |= col(column) < min_val
                range_msg = f"less than {min_val}"
            elif max_val is not None:
                condition |= col(column) > max_val
                range_msg = f"greater than {max_val}"
            else:
                range_msg = "not numeric"

            invalid_df = df_casted.filter(condition)
            invalid_count = invalid_df.count()

            if invalid_count > 0:
                return False, ExpectationResult(
                    f"Column '{column}' failed numeric/range validation ({range_msg} where {invalid_df.collect()})",
                    invalid_df
                )

            return True, ExpectationResult(f"Column '{column}' passed numeric and range validation")

        except Exception as e:
            return False, ExpectationResult(f"Validation error on column '{column}': {str(e)}")

    def diff_columns(self, df: DataFrame, col1: str, col2: str) -> Tuple[bool, ExpectationResult]:
        if col1 not in df.columns or col2 not in df.columns:
            return False, ExpectationResult(f"Missing one or both columns: '{col1}', '{col2}'")

        same_rows = df.filter(
            (col(col1) == col(col2)) | (col(col1).isNull() & col(col2).isNull())
        )

        count_same = same_rows.count()
        if count_same > 0:
            return False, ExpectationResult(f"Found {count_same} rows where '{col1}' == '{col2}'", same_rows)
        return True, ExpectationResult(f"All rows have different values for '{col1}' and '{col2}'")

    def team_unique_per_round(self, df: DataFrame, column: str) -> Tuple[bool, ExpectationResult]:
        duplicates = (
            df.groupBy("round", column)
            .agg(count("*").alias("count"))
            .filter(col("count") > 1)
        )

        dupe_count = duplicates.count()
        if dupe_count > 0:
            return False, ExpectationResult(
                f"Found {dupe_count} duplicate '{column}' values within the same round",
                duplicates
            )
        return True, ExpectationResult(f"Each '{column}' appears only once per round")

    def column_datetime_in_range(
        self,
        df: DataFrame,
        column: str,
        start_date: datetime,
        end_date: datetime
    ) -> Tuple[bool, ExpectationResult]:
        try:
            df_ts = df.withColumn("parsed_ts", to_timestamp(col(column)))

            invalid_ts = df_ts.filter(col("parsed_ts").isNull())
            invalid_ts_count = invalid_ts.count()
            if invalid_ts_count > 0:
                return False, ExpectationResult(f"Column '{column}' has {invalid_ts_count} unparseable datetime values", invalid_ts)

            out_of_range = df_ts.filter((col("parsed_ts") < start_date) | (col("parsed_ts") > end_date))
            out_of_range_count = out_of_range.count()
            if out_of_range_count > 0:
                return False, ExpectationResult(
                    f"Column '{column}' has {out_of_range_count} values outside allowed range [{start_date.date()} to {end_date.date()}]",
                    out_of_range
                )

            return True, ExpectationResult(f"✅ Column '{column}' values are valid datetimes within range")

        except Exception as e:
            return False, ExpectationResult(f"❌ Validation error on column '{column}': {str(e)}")

    def game_ids_match(self, df1: DataFrame, df2: DataFrame, id_column: str = "game_id") -> Tuple[bool, ExpectationResult]:
        ids1 = df1.select(id_column).distinct()
        ids2 = df2.select(id_column).distinct()

        only_in_2 = ids2.subtract(ids1)
        only_in_1 = ids1.subtract(ids2)

        missing_in_2 = only_in_2.count()
        missing_in_1 = only_in_1.count()

        if missing_in_1 == 0 and missing_in_2 == 0:
            return True, ExpectationResult(f"✅ Both DataFrames contain the same set of '{id_column}' values")

        list_2 = [row[id_column] for row in only_in_2.collect()]
        list_1 = [row[id_column] for row in only_in_1.collect()]

        return False, ExpectationResult(
            f"❌ Mismatch in '{id_column}':\n- {missing_in_2} missing in df1: {list_2}\n- {missing_in_1} missing in df2: {list_1}"
        )

    def row_count_match(self, df1: DataFrame, df2: DataFrame) -> Tuple[bool, ExpectationResult]:
        count1 = df1.count()
        count2 = df2.count()

        if count1 == count2:
            return True, ExpectationResult(f"✅ Row count matches: {count1} rows in both DataFrames")

        return False, ExpectationResult(
            f"❌ Row count mismatch:\n- df1: {count1} rows\n- df2: {count2} rows\n- Difference: {abs(count1 - count2)} row(s)"
        )