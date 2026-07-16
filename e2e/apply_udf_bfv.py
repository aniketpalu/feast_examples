"""Re-register udf_double_metrics with dill bytecode matching the Spark driver.

feast-apply runs in feature-server (often Python 3.12). The Spark driver image
is Python 3.10 — mismatched dill bytecodes fail at materialize with
SystemError/unknown opcode. Run this script inside the driver image (see
apply_udf_bfv_driver_job.yaml) after definitions.py is on the branch.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

CERT = os.environ.get(
    "FEAST_CA_CERT",
    "/var/run/secrets/kubernetes.io/serviceaccount/service-ca.crt",
)
REPO_URL = os.environ.get(
    "FEAST_EXAMPLES_GIT",
    "https://github.com/aniketpalu/feast_examples.git",
)
REPO_REF = os.environ.get("FEAST_EXAMPLES_REF", "byos-spark-postgres")


def _write_remote_yaml(repo: Path) -> None:
    (repo / "feature_store.yaml").write_text(
        f"""project: feast_spark_pg_e2e
provider: local
registry:
  registry_type: remote
  path: feast-spark-pg-e2e-registry.feast-spark.svc.cluster.local:443
  cert: {CERT}
online_store:
  type: remote
  path: https://feast-spark-pg-e2e-online.feast-spark.svc.cluster.local:443
  cert: {CERT}
offline_store:
  type: remote
  host: feast-spark-pg-e2e-offline.feast-spark.svc.cluster.local
  port: 443
  scheme: https
  cert: {CERT}
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
        # feast apply expects feature_store.yaml next to the project defs
        _write_remote_yaml(feature_repo)
        # Move yaml to parent layout feast expects: repo_path with feature_store.yaml
        # and definitions imported from cwd / feature_repo
        apply_root = work / "apply_root"
        apply_root.mkdir()
        shutil.copy(feature_repo / "feature_store.yaml", apply_root / "feature_store.yaml")
        shutil.copy(feature_repo / "definitions.py", apply_root / "definitions.py")

        os.chdir(apply_root)
        print("=== feast apply (driver Python) ===", flush=True)
        subprocess.check_call(["feast", "apply"])
        print("PASS: udf_double_metrics registered with driver-compatible dill", flush=True)
    finally:
        shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    main()
