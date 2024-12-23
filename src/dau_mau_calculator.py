from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, date_format, approx_count_distinct


def load_and_preprocess_data(spark, filepath):
    df = spark.read.parquet(filepath)
    df = df.withColumn("date", to_date(col("timestamp")))
    df = df.withColumn("month", date_format(col("timestamp"), "yyyy-MM"))
    return df


def calculate_dau(df):
    return df.groupBy("date").agg(
        approx_count_distinct("user_id").alias("daily_active_users")
    )


def calculate_mau(df):
    return df.groupBy("month").agg(
        approx_count_distinct("user_id").alias("monthly_active_users")
    )


if __name__ == "__main__":
    spark = SparkSession.builder.appName("DAU_MAU_Calculation").getOrCreate()

    df = load_and_preprocess_data(
        spark, filepath="data/user_interactions_sample.parquet"
    )

    dau = calculate_dau(df)
    print("Daily Active Users (DAU):")
    dau.orderBy("date").show()

    mau = calculate_mau(df)
    print("Monthly Active Users (MAU):")
    mau.orderBy("month").show()

    spark.stop()
