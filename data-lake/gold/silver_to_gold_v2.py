
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_date,
    avg,
    max as spark_max,
    count,
    concat_ws,
    current_timestamp,
    unix_timestamp,
)

sys.path.insert(0, "/tmp")
from config import configure_s3a, SILVER_V2_PATH, GOLD_V2_PATH

DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"

spark = (
    SparkSession.builder
    .appName("SmartGrid-Silver-V2-To-Gold-V2")
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

try:
    spark.sparkContext.setLogLevel("WARN")
    configure_s3a(spark)

    # Lecture de la table Silver Hudi
    silver = spark.read.format("hudi").load(SILVER_V2_PATH)

    # Calcul des indicateurs par quartier et par jour
    gold = (
        silver
        .filter(
            col("quartier").isNotNull()
            & col("event_time").isNotNull()
            & col("active_power").isNotNull()
        )
        .withColumn("date", to_date(col("event_time")))
        .groupBy("quartier", "date")
        .agg(
            count("*").alias("nombre_mesures"),
            avg("active_power").alias("puissance_moyenne"),
            spark_max("active_power").alias("puissance_max"),
        )
        .withColumn(
            "record_key",
            concat_ws("_", col("quartier"), col("date"))
        )
        .withColumn(
            "precombine_ts",
            unix_timestamp(current_timestamp()).cast("long")
        )
    )

    print("\n========== GOLD PREVIEW ==========")
    gold.printSchema()
    gold.show(20, truncate=False)

    nombre = gold.count()
    print("\nNombre de groupes Gold :", nombre)

    if DRY_RUN:
        print("\nGOLD_DRY_RUN_OK")
        print("Aucune ecriture effectuee.")

    elif nombre == 0:
        print("\nAucune donnee valide pour Gold.")

    else:
        (
            gold.write.format("hudi")
            .option(
                "hoodie.table.name",
                "consommation_quartier_v2"
            )
            .option(
                "hoodie.datasource.write.recordkey.field",
                "record_key"
            )
            .option(
                "hoodie.datasource.write.precombine.field",
                "precombine_ts"
            )
            .option(
                "hoodie.datasource.write.keygenerator.class",
                "org.apache.hudi.keygen.NonpartitionedKeyGenerator"
            )
            .option(
                "hoodie.datasource.write.operation",
                "upsert"
            )
            .option(
                "hoodie.datasource.write.table.type",
                "COPY_ON_WRITE"
            )
            .mode("append")
            .save(GOLD_V2_PATH)
        )

        print("\nGOLD_HUDI_OK")
        print("Destination :", GOLD_V2_PATH)

finally:
    spark.stop()
