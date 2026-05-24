from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, col

spark = SparkSession.builder.appName("silver_to_gold").getOrCreate()

df = spark.read.parquet("data/silver/")

kpi = df.groupBy("country", "device").agg(
    avg("session_duration").alias("avg_session"),
    count("*").alias("events_count")
)

kpi.write.mode("overwrite").parquet("data/gold/")