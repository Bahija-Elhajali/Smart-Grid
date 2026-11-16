
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, LongType
)

sys.path.insert(0, "/tmp")
from config import configure_s3a, HUDI_TEST_PATH


spark = (
    SparkSession.builder
    .appName("SmartGrid-Hudi-Versioning-Test")
    .master("local[2]")
    .config(
        "spark.serializer",
        "org.apache.spark.serializer.KryoSerializer"
    )
    .config(
        "spark.sql.extensions",
        "org.apache.spark.sql.hudi.HoodieSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.hudi.catalog.HoodieCatalog"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("record_key", StringType(), False),
    StructField("meter_id", StringType(), False),
    StructField("active_power", DoubleType(), False),
    StructField("revision", LongType(), False),
])

options = {
    "hoodie.table.name": "hudi_versioning_test",
    "hoodie.datasource.write.recordkey.field": "record_key",
    "hoodie.datasource.write.precombine.field": "revision",
    "hoodie.datasource.write.keygenerator.class":
        "org.apache.hudi.keygen.NonpartitionedKeyGenerator",
    "hoodie.datasource.write.operation": "upsert",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.write.record.merge.mode": "EVENT_TIME_ORDERING",
}

record_key = "TEST_M001_2026-11-15_22:30:00"


def write_measure(power, revision):
    df = spark.createDataFrame(
        [(record_key, "M001", float(power), int(revision))],
        schema
    )

    (
        df.write
        .format("hudi")
        .options(**options)
        .mode("append")
        .save(HUDI_TEST_PATH)
    )


def verify(expected_revision, expected_power):
    result = (
        spark.read
        .format("hudi")
        .load(HUDI_TEST_PATH)
        .select("record_key", "revision", "active_power")
        .collect()
    )

    assert len(result) == 1, (
        f"Nombre de lignes inattendu : {len(result)}"
    )

    row = result[0]

    assert row["revision"] == expected_revision, (
        f"Revision incorrecte : {row['revision']}"
    )

    assert abs(row["active_power"] - expected_power) < 0.0001, (
        f"Puissance incorrecte : {row['active_power']}"
    )

    print(
        f"VERIFICATION_OK : revision={row['revision']}, "
        f"active_power={row['active_power']}"
    )


try:
    configure_s3a(spark)

    # Utiliser un chemin de test neuf.
    # Ne pas supprimer une table existante.
    from py4j.java_gateway import java_import

    jvm = spark.sparkContext._jvm
    java_import(jvm, "org.apache.hadoop.fs.Path")

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    path = jvm.Path(HUDI_TEST_PATH)
    fs = path.getFileSystem(hadoop_conf)

    if fs.exists(path):
        raise RuntimeError(
            "Le chemin de test existe deja. "
            "Choisir un nouveau HUDI_TEST_PATH."
        )

    print("\n========== TEST 1 : INSERTION ==========")
    write_measure(2.45, 1)
    verify(1, 2.45)

    print("\n========== TEST 2 : CORRECTION ==========")
    write_measure(2.90, 2)
    verify(2, 2.90)

    print("\n========== TEST 3 : ANCIENNE VERSION ==========")
    write_measure(2.45, 1)
    verify(2, 2.90)

    print("\nHUDI_VERSIONING_TEST_OK")

finally:
    spark.stop()
