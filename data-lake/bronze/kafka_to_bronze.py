
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

sys.path.insert(0, "/tmp")
from config import configure_s3a

spark = (
    SparkSession.builder
    .appName("SmartGrid-Kafka-To-Bronze")
    .master("local[2]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
configure_s3a(spark)

bucket = os.getenv("S3_BUCKET", "smartgrid-lake")

bronze_path = f"s3a://{bucket}/bronze/releves/"
checkpoint_path = f"s3a://{bucket}/checkpoints/bronze_kafka/"

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "releves")
    .option("startingOffsets", "earliest")
    .load()
)

bronze_df = kafka_df.select(
    col("value").cast("string").alias("raw_json"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp").alias("kafka_timestamp")
)

query = (
    bronze_df.writeStream
    .format("json")
    .outputMode("append")
    .option("path", bronze_path)
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()

print("BRONZE_INGESTION_OK")
print("Destination :", bronze_path)

spark.stop()
