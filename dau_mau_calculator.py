from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, date_format, approx_count_distinct

spark = SparkSession.builder \
    .appName("DAU_MAU_Calculation") \
    .getOrCreate()

df = spark.read.parquet('user_interactions_sample.parquet')

df = df.withColumn('date', to_date(col('timestamp')))
df = df.withColumn('month', date_format(col('timestamp'), 'yyyy-MM'))

# Calculate DAU
dau = df.groupBy('date') \
    .agg(approx_count_distinct('user_id').alias('daily_active_users'))

# Calculate MAU
mau = df.groupBy('month') \
    .agg(approx_count_distinct('user_id').alias('monthly_active_users'))

print("Daily Active Users (DAU):")
dau.orderBy('date').show()

print("Monthly Active Users (MAU):")
mau.orderBy('month').show()

spark.stop()