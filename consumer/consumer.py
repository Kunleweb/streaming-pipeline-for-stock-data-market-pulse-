from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import from_json, col, to_timestamp
import os
import pyspark

# This reads the installed PySpark version so your connector stays aligned.
spark_version = pyspark.__version__

# Windows-friendly checkpoint folder (relative path is easiest).
checkpoint_dir = os.path.join(".", "checkpoint", "kafka_to_postgres")
os.makedirs(checkpoint_dir, exist_ok=True)

kafka_data_schema = StructType([
    StructField("date", StringType()),
    StructField("high", StringType()),
    StructField("low", StringType()),
    StructField("open", StringType()),
    StructField("close", StringType()),
    StructField("symbol", StringType())
])

spark = (
    SparkSession.builder
    .appName("KafkaSparkStreaming")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# IMPORTANT:
# - If this script runs on your Windows host and Kafka is in Docker, you often need localhost:9092
# - If this script runs inside a container on the same docker network, kafka:9092 is fine.
bootstrap = "kafka:9092"

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", bootstrap)
    .option("subscribe", "stock_analysis")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

parsed_df = (
    df.selectExpr("CAST(value AS STRING) AS value")
    .select(from_json(col("value"), kafka_data_schema).alias("data"))
    .select("data.*")
)

processed_df = parsed_df.select(
    # If your date string is like "2026-02-20 10:30:00" adjust the format string accordingly.
    to_timestamp(col("date"), "yyyy-MM-dd'T'HH:mm:ss").alias("date"),
    col("high"),
    col("low"),
    col("open"),
    col("close"),
    col("symbol"),
)

query = (
    processed_df.writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", "false")
    .option("checkpointLocation", checkpoint_dir)
    .start()
)

query.awaitTermination()