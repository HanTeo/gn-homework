from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, date_format, approx_count_distinct


def calculate_dau(df):
    """
    Calculates DAU by grouping on a 'date' column derived from 'timestamp',
    and counting distinct user_id using an approximate function.
    """
    # Convert 'timestamp' to a date for grouping.
    df_date = df.withColumn("date", to_date(col("timestamp")))

    # Group by date and approximate-count distinct user_id
    return df_date.groupBy("date").agg(
        approx_count_distinct("user_id").alias("daily_active_users")
    )


def calculate_mau(df):
    """
    Calculates MAU by grouping on a 'month' column derived from 'timestamp',
    and counting distinct user_id using an approximate function.
    """
    # Convert 'timestamp' to a YYYY-MM string for grouping.
    df_month = df.withColumn("month", date_format(col("timestamp"), "yyyy-MM"))

    # Group by month and approximate-count distinct user_id
    return df_month.groupBy("month").agg(
        approx_count_distinct("user_id").alias("monthly_active_users")
    )


if __name__ == "__main__":
    spark = SparkSession.builder.appName("DAU_MAU_Calculation").getOrCreate()

    # 1. Read only the columns needed
    df = (
        spark.read.parquet("data/user_interactions_sample.parquet")
        .select("user_id", "timestamp")
        .withColumn("timestamp", col("timestamp").cast("timestamp"))
    )

    # 2. Calculate DAU
    dau = calculate_dau(df)
    print("Daily Active Users (DAU):")
    dau.orderBy("date").show()

    # 3. Calculate MAU
    mau = calculate_mau(df)
    print("Monthly Active Users (MAU):")
    mau.orderBy("month").show()

    spark.stop()
