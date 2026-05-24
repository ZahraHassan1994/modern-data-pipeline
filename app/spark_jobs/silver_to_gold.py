from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count

spark = SparkSession.builder \
    .appName("SilverToGold") \
    .getOrCreate()

df = spark.read.parquet(
    "data/silver/"
)

gold_df = df.groupBy("country").agg(
    avg("session_duration").alias("avg_session_duration"),
    count("*").alias("total_events")
)

gold_df.write.mode("overwrite").parquet(
    "data/gold/"
)

