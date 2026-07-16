"""Re-register udf_double_metrics with dill bytecode matching the Spark driver.

Uses the same SQL registry + Postgres/Redis stores as the FeatureStore servers
(not remote registry) so ApplyFeatureView does not require the feast pod's
checked-out definitions.py to unpickle the UDF.

feast-apply in feature-server is often a different Python than the Spark driver
image — mismatched dill bytecodes fail at materialize. Run this inside the
driver image (apply_udf_bfv_driver_job.yaml).
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

# Defaults match feast_examples postgres E2E (setup_postgres_small.py).
PG_USER = os.environ.get("PG_USER", "feast")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "feast")
PG_HOST = os.environ.get("PG_HOST", "postgres")
PG_PORT = os.environ.get("PG_PORT", "5432")
PG_DB = os.environ.get("PG_DB", "feast")
REDIS = os.environ.get("REDIS_CONNECTION_STRING", "redis:6379")
REPO_URL = os.environ.get(
    "FEAST_EXAMPLES_GIT",
    "https://github.com/aniketpalu/feast_examples.git",
)
REPO_REF = os.environ.get("FEAST_EXAMPLES_REF", "byos-spark-postgres")


def _write_sql_yaml(repo: Path) -> None:
    registry = (
        f"postgresql+psycopg://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    )
    (repo / "feature_store.yaml").write_text(
        f"""project: feast_spark_pg_e2e
provider: local
offline_store:
  type: postgres
  host: {PG_HOST}
  port: {PG_PORT}
  database: {PG_DB}
  db_schema: public
  user: {PG_USER}
  password: {PG_PASSWORD}
  sslmode: disable
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
    work = Path(tempfile.mkdtemp(prefix="udf-bfv-"))
    try:
        clone = work / "feast_examples"
        subprocess.check_call(
            ["git", "clone", "--depth", "1", "--branch", REPO_REF, REPO_URL, str(clone)],
        )
        feature_repo = clone / "feature_repo"
        apply_root = work / "apply_root"
        apply_root.mkdir()
        _write_sql_yaml(apply_root)
        shutil.copy(feature_repo / "definitions.py", apply_root / "definitions.py")

        os.chdir(apply_root)
        print("=== feast apply via SQL registry (driver Python) ===", flush=True)
        subprocess.check_call(["feast", "apply"])
        print("PASS: udf_double_metrics registered with driver-compatible dill", flush=True)
    finally:
        shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    main()
