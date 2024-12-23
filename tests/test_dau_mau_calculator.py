import pytest
from pyspark.sql import SparkSession
from chispa.dataframe_comparer import assert_df_equality
from datetime import datetime, date
from src.dau_mau_calculator import calculate_dau, preprocess_data
from pyspark.sql.functions import col, to_date, date_format, approx_count_distinct


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder.appName("DAUMAUCalcTest").master("local[*]").getOrCreate()
    )
    yield spark
    spark.stop()


def test_calculate_dau(spark):
    input_data = [
        ("u000001", datetime(2023, 1, 1, 10, 0, 0)),
        ("u000002", datetime(2023, 1, 1, 11, 0, 0)),
        ("u000001", datetime(2023, 1, 2, 9, 30, 0)),
        ("u000003", datetime(2023, 1, 2, 15, 0, 0)),
    ]

    df = spark.createDataFrame(input_data, ["user_id", "timestamp"])

    df = preprocess_data(df)

    result_df = calculate_dau(df)

    expected_data = [(date(2023, 1, 1), 2), (date(2023, 1, 2), 2)]
    expected_df = spark.createDataFrame(expected_data, ["date", "daily_active_users"])

    assert_df_equality(
        expected_df,
        result_df,
        ignore_nullable=True,
    )
