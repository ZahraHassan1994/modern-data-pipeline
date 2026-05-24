from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("bronze_to_silver").getOrCreate()

df = spark.read.csv("data/bronze/events.csv", header=True, inferSchema=True)

clean_df = df.filter(
    col("user_id").isNotNull() &
    col("session_duration").isNotNull()
)

clean_df = clean_df.dropDuplicates()

clean_df.write.mode("overwrite").parquet("data/silver/")