from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lag,
    unix_timestamp,
    when,
    sum as spark_sum,
    min as spark_min,
    max as spark_max,
    count,
    avg,
    to_date,
)
from pyspark.sql.window import Window


def calculate_session_metrics(df, inactivity_threshold=3000):
    """
    Given a DataFrame with columns [user_id, timestamp, ...],
    calculates session metrics including:
      - session_id
      - session_start, session_end, session_duration_sec
      - actions_in_session
    Returns three outputs:
      1) session-level DataFrame
      2) overall metrics (average duration & actions)
      3) daily session metrics by date
    """
    # Window for ordering by user_id, timestamp
    window_spec = Window.partitionBy("user_id").orderBy("timestamp")

    # Identify session boundaries
    df = (
        df.withColumn("prev_timestamp", lag("timestamp").over(window_spec))
        .withColumn(
            "time_diff", unix_timestamp("timestamp") - unix_timestamp("prev_timestamp")
        )
        .withColumn(
            "new_session",
            when(
                (col("time_diff") >= inactivity_threshold) | col("time_diff").isNull(),
                1,
            ).otherwise(0),
        )
        # Cumulative sum of new_session to assign session_id
        .withColumn("session_id", spark_sum("new_session").over(window_spec))
    )

    # Window to aggregate by (user_id, session_id)
    session_window = Window.partitionBy("user_id", "session_id")

    df = (
        df.withColumn("session_start", spark_min("timestamp").over(session_window))
        .withColumn("session_end", spark_max("timestamp").over(session_window))
        .withColumn(
            "session_duration_sec",
            unix_timestamp("session_end") - unix_timestamp("session_start"),
        )
    )

    # Count how many actions occur in each session
    session_action_counts = df.groupBy("user_id", "session_id").agg(
        count("*").alias("actions_in_session")
    )

    # Build a session-level DataFrame
    session_df = (
        df.select(
            "user_id",
            "session_id",
            "session_start",
            "session_end",
            "session_duration_sec",
        )
        .dropDuplicates(["user_id", "session_id"])
        .join(session_action_counts, on=["user_id", "session_id"], how="left")
    )

    # Overall metrics across all sessions
    overall_metrics = session_df.agg(
        avg("session_duration_sec").alias("avg_session_duration_sec"),
        avg("actions_in_session").alias("avg_actions_per_session"),
    )

    # Daily session metrics (group by date derived from session_start)
    session_df = session_df.withColumn("date", to_date("session_start"))
    daily_session_metrics = (
        session_df.groupBy("date")
        .agg(
            avg("session_duration_sec").alias("daily_avg_session_duration_sec"),
            avg("actions_in_session").alias("daily_avg_actions_per_session"),
        )
        .orderBy("date")
    )

    return session_df, overall_metrics, daily_session_metrics


def preprocess_data(df):
    """
    Select & cast only necessary columns if possible, to reduce I/O and speed up transformations.
    """
    return df.select("user_id", "timestamp").withColumn(
        "timestamp", col("timestamp").cast("timestamp")
    )


if __name__ == "__main__":
    spark = SparkSession.builder.appName("SessionMetricsCalculator").getOrCreate()

    df = spark.read.parquet("data/user_interactions_sample.parquet")

    df = preprocess_data(df)

    session_df, overall, daily = calculate_session_metrics(
        df, inactivity_threshold=3000
    )

    print("Session DataFrame:")
    session_df.show(20, truncate=False)

    print("Overall Metrics:")
    overall.show()

    print("Daily Session Metrics:")
    daily.show(20, truncate=False)

    spark.stop()
