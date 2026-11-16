
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat_ws,
    from_json,
    to_timestamp,
    unix_timestamp,
    lit,
    coalesce,
    date_format,
    when,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
)

sys.path.insert(0, "/tmp")
from config import configure_s3a, BRONZE_PATH, SILVER_PATH


# Mode test active par defaut
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"

# Protection temporaire des corrections Hudi existantes
ALLOW_SILVER_WRITE = False


spark = (
    SparkSession.builder
    .appName("SmartGrid-Bronze-To-Silver")
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

try:
    # 1. Configuration SILO / S3
    configure_s3a(spark)

    # 2. Schema des mesures Kafka
    schema = StructType([
        StructField("meter_id", StringType()),
        StructField("quartier", StringType()),
        StructField("timestamp", StringType()),
        StructField("active_power", DoubleType()),
        StructField("voltage", DoubleType()),
        StructField("intensity", DoubleType()),
        StructField("is_anomaly", LongType()),
    ])

    # 3. Lecture des donnees Bronze
    bronze = spark.read.json(
        BRONZE_PATH + "releves/"
    )

    # 4. Parsing du JSON
    parsed = (
        bronze
        .withColumn(
            "data",
            from_json(col("raw_json"), schema)
        )
        .select("data.*")
    )

    # 5. Normalisation des dates
    #
    # Format ISO :
    # 2026-11-15 22:30:00
    #
    # Format dataset :
    # 15/11/2026 22:30:00

    parsed = parsed.withColumn(
        "event_time",
        when(
            col("timestamp").rlike(
                r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$"
            ),
            to_timestamp(
                col("timestamp"),
                "yyyy-MM-dd HH:mm:ss"
            )
        ).when(
            col("timestamp").rlike(
                r"^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$"
            ),
            to_timestamp(
                col("timestamp"),
                "dd/MM/yyyy HH:mm:ss"
            )
        )
    )

    # 6. Nettoyage et validation
    silver = (
        parsed
        .filter(
            col("meter_id").isNotNull()
            & (col("meter_id") != "")
            & col("quartier").isNotNull()
            & (col("quartier") != "")
            & col("event_time").isNotNull()
            & col("active_power").isNotNull()
            & (col("active_power") >= 0)
            & col("voltage").isNotNull()
            & (col("voltage") >= 0)
            & col("intensity").isNotNull()
            & (col("intensity") >= 0)
        )
        .withColumn(
            "record_key",
            concat_ws(
                "_",
                col("meter_id"),
                date_format(
                    col("event_time"),
                    "yyyy-MM-dd HH:mm:ss"
                )
            )
        )
        .withColumn(
            "precombine_ts",
            unix_timestamp(
                col("event_time")
            ).cast("long")
        )
        .withColumn(
            "source",
            lit("kafka")
        )
    )

    # 7. Apercu Silver
    print("\n========== SILVER PREVIEW ==========")

    silver.printSchema()
    silver.show(20, truncate=False)

    # 8. Verification des cles Hudi
    print("\n========== HUDI KEYS CHECK ==========")

    silver.select(
        "record_key",
        "precombine_ts",
        "source"
    ).show(20, truncate=False)

    count = silver.count()

    print("\nNombre de mesures valides :", count)

    # 9. Mode test
    if DRY_RUN:
        print("\n========== DRY RUN ==========")
        print("DRY_RUN_OK")
        print("Aucune ecriture Hudi effectuee.")

    elif not ALLOW_SILVER_WRITE:
        raise RuntimeError(
            "ECRITURE_SILVER_BLOQUEE : "
            "le versioning des corrections Hudi "
            "doit etre securise avant toute ecriture."
        )

    elif count == 0:
        print(
            "Aucune mesure valide : "
            "ecriture ignoree."
        )

    else:
        # 10. Upsert Silver Hudi
        (
            silver.write
            .format("hudi")
            .option(
                "hoodie.table.name",
                "mesures_silver"
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
            .save(SILVER_PATH)
        )

        print("\nSILVER_HUDI_OK")
        print("Destination :", SILVER_PATH)

finally:
    spark.stop()
