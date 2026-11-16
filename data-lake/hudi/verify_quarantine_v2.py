import os
from pyspark.sql import SparkSession
from config import configure_s3a, QUARANTINE_V2_PATH

spark = (
    SparkSession.builder
    .appName("VerifyQuarantine")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()
)

configure_s3a(spark)

try:
    df = spark.read.format("hudi").load(QUARANTINE_V2_PATH)

    df.select(
        "record_key",
        "revision",
        "active_power",
        "reason",
        "quarantine_version"
    ).show(truncate=False)

    count = df.count()
    print("QUARANTINE_COUNT =", count)

    if count != 2:
        raise RuntimeError(f"Expected 2 records, found {count}")

    print("QUARANTINE_VERIFICATION_OK")
finally:
    spark.stop()
