import pytest
from datetime import datetime, date
from pyspark.sql import SparkSession
from chispa.dataframe_comparer import assert_df_equality

from src.session_metrics_calculator import calculate_session_metrics


@pytest.fixture(scope="session")
def spark():
    """
    Creates a SparkSession for testing in local mode.
    After tests complete, this session is stopped.
    """
    spark = (
        SparkSession.builder.appName("SessionCalculatorTests")
        .master("local[*]")
        .getOrCreate()
    )
    yield spark
    spark.stop()


def test_calculate_sessions_simple(spark):
    """
    Verify that sessions are calculated correctly when events
    are within or beyond the 30-minute threshold.
    """
    # 1. Build a small input DataFrame
    #    user1 has 2 events within 5 min => same session
    #    then 1 event 55 min later => new session
    #    user2 has just 1 event
    data = [
        ("user1", datetime(2023, 1, 1, 10, 0, 0), "view"),
        ("user1", datetime(2023, 1, 1, 10, 5, 0), "view"),
        ("user1", datetime(2023, 1, 1, 11, 0, 0), "edit"),
        ("user2", datetime(2023, 1, 1, 8, 0, 0), "view"),
    ]
    input_df = spark.createDataFrame(data, ["user_id", "timestamp", "action_type"])

    # 2. Calculate sessions with default threshold of 1800 seconds (30 min)
    result_df, _, _ = calculate_session_metrics(input_df, inactivity_threshold=1800)

    # 3. Build an expected DataFrame (one row per session)
    # user1 =>
    #   session_id=1 with 2 events (10:00 - 10:05 => 5 min => 300s)
    #   session_id=2 with 1 event (11:00 - 11:00 => 0)
    # user2 =>
    #   session_id=1 with 1 event (08:00 - 08:00 => 0)
    columns = [
        "user_id",
        "session_id",
        "session_start",
        "session_end",
        "session_duration_sec",
        "actions_in_session",
        "date",
    ]
    expected_data = [
        (
            "user1",
            1,
            datetime(2023, 1, 1, 10, 0, 0),
            datetime(2023, 1, 1, 10, 5, 0),
            300,
            2,
            date(2023, 1, 1),
        ),
        (
            "user1",
            2,
            datetime(2023, 1, 1, 11, 0, 0),
            datetime(2023, 1, 1, 11, 0, 0),
            0,
            1,
            date(2023, 1, 1),
        ),
        (
            "user2",
            1,
            datetime(2023, 1, 1, 8, 0, 0),
            datetime(2023, 1, 1, 8, 0, 0),
            0,
            1,
            date(2023, 1, 1),
        ),
    ]
    expected_df = spark.createDataFrame(expected_data, columns)

    assert_df_equality(
        result_df,
        expected_df,
        ignore_row_order=True,
        ignore_nullable=True,
    )
