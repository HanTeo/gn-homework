import pytest
from pyspark.sql import SparkSession
from chispa.dataframe_comparer import assert_df_equality
from datetime import datetime
from src.dau_mau_calculator import calculate_dau, calculate_mau


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

    result_df = calculate_dau(df)

    expected_data = [("2023-01-01", 2), ("2023-01-02", 2)]
    expected_df = spark.createDataFrame(expected_data, ["date", "daily_active_users"])

    assert_df_equality(
        result_df,
        expected_df,
        ignore_column_order=True,
        ignore_row_order=True,
        ignore_nullable=True,
    )
