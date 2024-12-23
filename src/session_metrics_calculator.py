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
    window_spec = Window.partitionBy("user_id").orderBy("timestamp")

    df = df.withColumn("prev_timestamp", lag("timestamp").over(window_spec))
    df = df.withColumn(
        "time_diff", unix_timestamp("timestamp") - unix_timestamp("prev_timestamp")
    )

    df = df.withColumn(
        "new_session",
        when(
            (col("time_diff") >= inactivity_threshold) | col("time_diff").isNull(), 1
        ).otherwise(0),
    )

    df = df.withColumn("session_id", spark_sum("new_session").over(window_spec))

    session_window = Window.partitionBy("user_id", "session_id")

    df = df.withColumn("session_start", spark_min("timestamp").over(session_window))
    df = df.withColumn("session_end", spark_max("timestamp").over(session_window))
    df = df.withColumn(
        "session_duration_sec",
        unix_timestamp("session_end") - unix_timestamp("session_start"),
    )

    session_action_counts = df.groupBy("user_id", "session_id").agg(
        count("*").alias("actions_in_session")
    )

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

    overall_metrics = session_df.agg(
        avg("session_duration_sec").alias("avg_session_duration_sec"),
        avg("actions_in_session").alias("avg_actions_per_session"),
    )

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


def load_and_preprocess_data(spark, filepath):
    df = spark.read.parquet(filepath)
    df = df.withColumn("timestamp", col("timestamp").cast("timestamp"))
    return df


if __name__ == "__main__":
    spark = SparkSession.builder.appName("SessionMetricsCalculator").getOrCreate()
    df = load_and_preprocess_data(
        spark, filepath="data/user_interactions_sample.parquet"
    )
    session_df, overall, daily = calculate_session_metrics(df=df)

    print("Session Data Frame")
    session_df.show(20)
    print("Overall Metrics")
    overall.show()
    print("Session Metrics")
    daily.show(20)
    spark.stop()
