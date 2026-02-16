from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Spark session
spark = SparkSession.builder \
    .appName("KafkaRealTimeETL") \
    .getOrCreate()

# Kafka topic
kafka_bootstrap = "localhost:9092"
topic = "realtime-events"

# Define schema of incoming data
schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("event_type", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("timestamp", IntegerType(), True),
])

# Read streaming data from Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap) \
    .option("subscribe", topic) \
    .option("startingOffsets", "latest") \
    .load()

# Convert the binary value column to string and parse JSON
df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

# Example transformation: filter purchase events only
df_filtered = df_parsed.filter(col("event_type") == "purchase")

# Write stream to console (for testing)
query = df_filtered.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
