
import os
import subprocess
from pathlib import Path

# Charger les variables depuis .env
env_file = Path(".env")

if not env_file.exists():
    raise FileNotFoundError("Fichier .env introuvable")

for line in env_file.read_text(encoding="utf-8").splitlines():
    line = line.strip()

    if not line or line.startswith("#"):
        continue

    key, separator, value = line.partition("=")

    if separator:
        os.environ[key.strip()] = value.strip().strip('"').strip("'")

# Verifier les identifiants SILO
for key in ("MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD"):
    if not os.getenv(key):
        raise RuntimeError(f"Variable manquante : {key}")

# Safe Mode par defaut
dry_run = os.getenv("DRY_RUN", "true").lower()

if dry_run not in ("true", "false"):
    raise ValueError("DRY_RUN doit etre true ou false")

jars = [
    "/tmp/hudi-spark3.5-bundle_2.12-1.0.2.jar",
    "/tmp/hadoop-aws-3.3.4.jar",
    "/tmp/aws-java-sdk-bundle-1.12.262.jar",
]

command = [
    "docker", "exec",
    "-e", "MINIO_ROOT_USER",
    "-e", "MINIO_ROOT_PASSWORD",
    "-e", f"DRY_RUN={dry_run}",
    "spark-master",
    "/opt/spark/bin/spark-submit",
    "--master", "local[2]",
    "--jars", ",".join(jars),
    "/tmp/silver_to_gold.py",
]

print(f"Silver vers Gold - DRY_RUN={dry_run}", flush=True)

subprocess.run(command, check=True)
