
import os
import subprocess
from pathlib import Path

project_root = Path(__file__).resolve().parents[2]
env_file = project_root / ".env"

credentials = {}

for line in env_file.read_text(encoding="utf-8").splitlines():
    line = line.strip()
    if not line or line.startswith("#"):
        continue

    key, separator, value = line.partition("=")
    if separator and key.strip() in {
        "MINIO_ROOT_USER",
        "MINIO_ROOT_PASSWORD",
    }:
        credentials[key.strip()] = value.strip().strip('"').strip("'")

for key in ("MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD"):
    if not credentials.get(key):
        raise RuntimeError(f"Variable manquante : {key}")

jars = [
    "/tmp/spark-sql-kafka-0-10_2.12-3.5.1.jar",
    "/tmp/spark-token-provider-kafka-0-10_2.12-3.5.1.jar",
    "/tmp/kafka-clients-3.4.1.jar",
    "/tmp/commons-pool2-2.11.1.jar",
    "/tmp/hadoop-aws-3.3.4.jar",
    "/tmp/aws-java-sdk-bundle-1.12.262.jar",
]

command = [
    "docker", "exec", "-i",
    "-e", "MINIO_ROOT_USER",
    "-e", "MINIO_ROOT_PASSWORD",
    "spark-master",
    "/opt/spark/bin/spark-submit",
    "--master", "local[2]",
    "--jars", ",".join(jars),
    "/tmp/kafka_to_bronze.py",
]

environment = os.environ.copy()
environment.update(credentials)

print("Lancement Kafka -> Spark -> Bronze...")
subprocess.run(command, env=environment, check=True)
