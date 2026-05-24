from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("BronzeToSilver") \
    .getOrCreate()

df = spark.read.csv(
    "data/bronze/events.csv",
    header=True,
    inferSchema=True
)

clean_df = df.dropna()

clean_df.write.mode("overwrite").parquet(
    "data/silver/"
)

print("Silver layer created successfully")