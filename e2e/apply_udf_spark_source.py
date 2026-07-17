"""Re-register SparkSource BFV UDF with driver Python (dill match)."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

PG_USER = os.environ.get("PG_USER", "feast")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "feast")
PG_HOST = os.environ.get("PG_HOST", "postgres")
PG_PORT = os.environ.get("PG_PORT", "5432")
PG_DB = os.environ.get("PG_DB", "feast")
REDIS = os.environ.get("REDIS_CONNECTION_STRING", "redis:6379")
MINIO = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
REPO_URL = os.environ.get(
    "FEAST_EXAMPLES_GIT",
    "https://github.com/aniketpalu/feast_examples.git",
)
REPO_REF = os.environ.get("FEAST_EXAMPLES_REF", "byos-spark-postgres")
FEATURE_REPO = os.environ.get(
    "FEAST_FEATURE_REPO", "feature_repo_spark_source_udf"
)


def _write_yaml(repo: Path) -> None:
    registry = (
        f"postgresql+psycopg://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    )
    (repo / "feature_store.yaml").write_text(
        f"""project: feast_spark_src_udf
provider: local
offline_store:
  type: spark
  spark_conf:
    spark.master: local[*]
    spark.hadoop.fs.s3a.endpoint: "{MINIO}"
    spark.hadoop.fs.s3a.access.key: minioadmin
    spark.hadoop.fs.s3a.secret.key: minioadmin
    spark.hadoop.fs.s3a.path.style.access: "true"
    spark.hadoop.fs.s3a.impl: org.apache.hadoop.fs.s3a.S3AFileSystem
    spark.hadoop.fs.s3a.connection.ssl.enabled: "false"
online_store:
  type: redis
  connection_string: {REDIS}
registry:
  registry_type: sql
  path: {registry}
entity_key_serialization_version: 3
auth:
  type: no_auth
"""
    )


def main() -> None:
    print(f"python={sys.version}", flush=True)
    work = Path(tempfile.mkdtemp(prefix="udf-ss-"))
    try:
        clone = work / "feast_examples"
        subprocess.check_call(
            ["git", "clone", "--depth", "1", "--branch", REPO_REF, REPO_URL, str(clone)],
        )
        feature_repo = clone / FEATURE_REPO
        apply_root = work / "apply_root"
        apply_root.mkdir()
        _write_yaml(apply_root)
        shutil.copy(feature_repo / "definitions.py", apply_root / "definitions.py")
        os.chdir(apply_root)
        print("=== feast apply (SparkSource UDF, driver Python) ===", flush=True)
        subprocess.check_call(["feast", "apply"])
        print("PASS: SparkSource UDF registered", flush=True)
    finally:
        shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    main()
