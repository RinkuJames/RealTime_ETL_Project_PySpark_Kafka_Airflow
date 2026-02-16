# spark/streaming_job.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# ==========================================================
# 1️⃣ Create Spark Session
# ==========================================================

spark = SparkSession.builder \
    .appName("RealTimeEcommerceETL") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ==========================================================
# 2️⃣ Define Schema (Strict Schema Enforcement)
# ==========================================================

schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("country", StringType(), True),
    StructField("event_time", StringType(), True)
])

# ==========================================================
# 3️⃣ Read Stream From Kafka
# ==========================================================

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ecommerce_events") \
    .option("startingOffsets", "latest") \
    .load()

# ==========================================================
# 🥉 BRONZE LAYER (Raw Ingestion)
# ==========================================================

raw_df = kafka_df.selectExpr("CAST(value AS STRING) as raw_json")

raw_query = raw_df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", "data/raw") \
    .option("checkpointLocation", "data/checkpoint/raw") \
    .start()

# ==========================================================
# Parse JSON
# ==========================================================

parsed_df = kafka_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# ==========================================================
# 🥈 SILVER LAYER (Clean & Transform)
# ==========================================================

clean_df = parsed_df \
    .withColumn("event_time", to_timestamp("event_time")) \
    .withColumn("ingestion_time", current_timestamp()) \
    .dropna()

clean_query = clean_df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", "data/clean") \
    .option("checkpointLocation", "data/checkpoint/clean") \
    .start()

# ==========================================================
# 🥇 GOLD LAYER (Windowed Aggregation)
# ==========================================================

agg_df = clean_df \
    .withWatermark("event_time", "2 minutes") \
    .groupBy(
        window(col("event_time"), "1 minute"),
        col("category")
    ) \
    .agg(
        sum("price").alias("total_revenue"),
        count("*").alias("total_orders")
    )

gold_query = agg_df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", "data/gold") \
    .option("checkpointLocation", "data/checkpoint/gold") \
    .start()

# ==========================================================
# Keep Streams Running
# ==========================================================

spark.streams.awaitAnyTermination()
