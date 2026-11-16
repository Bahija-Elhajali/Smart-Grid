
import os

# S3-compatible storage (MinIO / SILO)
S3_ENDPOINT = os.getenv(
    "S3_ENDPOINT",
    "http://host.docker.internal:9000"
)

S3_ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
S3_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")

BUCKET = os.getenv("S3_BUCKET", "smartgrid-lake")

BRONZE_PATH = f"s3a://{BUCKET}/bronze/"
SILVER_PATH = f"s3a://{BUCKET}/silver/mesures_hudi"
GOLD_PATH = f"s3a://{BUCKET}/gold/consommation_quartier"
HUDI_TEST_PATH = f"s3a://{BUCKET}/tests/hudi_versioning"

def configure_s3a(spark):
    """Configure Spark to access the S3-compatible Data Lake."""

    if not S3_ACCESS_KEY or not S3_SECRET_KEY:
        raise RuntimeError(
            "MINIO_ROOT_USER et MINIO_ROOT_PASSWORD "
            "doivent etre definis dans l'environnement."
        )

    conf = spark.sparkContext._jsc.hadoopConfiguration()

    conf.set(
        "fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )
    conf.set("fs.s3a.endpoint", S3_ENDPOINT)
    conf.set("fs.s3a.path.style.access", "true")
    conf.set("fs.s3a.connection.ssl.enabled", "false")
    conf.set("fs.s3a.access.key", S3_ACCESS_KEY)
    conf.set("fs.s3a.secret.key", S3_SECRET_KEY)
    conf.set(
        "fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    )

SILVER_VERSIONING_TEST_PATH = f"s3a://{BUCKET}/tests/silver_revision_integration_v2"

SILVER_V2_PATH = f"s3a://{BUCKET}/silver/mesures_hudi_v2"

GOLD_V2_PATH = f's3a://{BUCKET}/gold/consommation_quartier_v2'
