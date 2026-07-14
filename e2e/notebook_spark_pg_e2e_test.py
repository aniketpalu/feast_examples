"""Notebook / workbench client for spark-pg-e2e (functionality-first).

Expects Feast SDK from aniketpalu/feast@merged-spark-e2e installed in the
environment (or PYTHONPATH). Uses remote materialize via online_store.path + cert.

Friction-finding mode: print clear PASS/FAIL per step; do not swallow errors.
"""

from __future__ import annotations

import os
import sys
import time
from datetime import datetime, timezone

from feast import FeatureStore

NS = os.environ.get("FEAST_NS", "feast-spark")
CR_NAME = os.environ.get("FEAST_CR", "spark-pg-e2e")
PROJECT = os.environ.get("FEAST_PROJECT", "feast_spark_pg_e2e")
ONLINE = os.environ.get(
    "FEAST_ONLINE_URL",
    f"https://feast-{CR_NAME}-online.{NS}.svc.cluster.local:443",
)
CERT = os.environ.get(
    "FEAST_CA_CERT",
    "/var/run/secrets/kubernetes.io/serviceaccount/service-ca.crt",
)
REPO_DIR = os.environ.get("FEAST_REPO_DIR", "/tmp/spark-pg-e2e-client")

# Small functionality set
FV_NAMES = ["feature_view_1", "feature_view_2"]
START = datetime(2024, 1, 1, tzinfo=timezone.utc)
END = datetime(2024, 1, 15, tzinfo=timezone.utc)


def write_client_yaml(path: str) -> None:
    os.makedirs(path, exist_ok=True)
    yaml = f"""project: {PROJECT}
provider: local
registry:
  registry_type: remote
  path: feast-{CR_NAME}-registry.{NS}.svc.cluster.local:443
  cert: {CERT}
online_store:
  type: remote
  path: {ONLINE}
  cert: {CERT}
offline_store:
  type: remote
  host: feast-{CR_NAME}-offline.{NS}.svc.cluster.local
  port: 443
  scheme: https
  cert: {CERT}
entity_key_serialization_version: 3
"""
    with open(os.path.join(path, "feature_store.yaml"), "w") as f:
        f.write(yaml)


def step(name: str):
    print(f"\n=== STEP: {name} ===", flush=True)


def main() -> int:
    write_client_yaml(REPO_DIR)
    store = FeatureStore(repo_path=REPO_DIR)

    step("list feature views")
    fvs = store.list_feature_views()
    names = sorted(fv.name for fv in fvs)
    print(f"found {len(names)}: {names}")
    missing = [n for n in FV_NAMES if n not in names]
    if missing:
        print(f"FRICTION: expected FVs missing: {missing}")
        return 1
    print("PASS: required FVs present")

    step("remote materialize (wait=True)")
    t0 = time.time()
    try:
        store.materialize(START, END, feature_views=FV_NAMES, remote=True)
        print(f"PASS: materialize returned in {time.time() - t0:.1f}s")
    except Exception as e:
        print(f"FRICTION/FAIL materialize: {type(e).__name__}: {e}")
        return 2

    step("get online features")
    try:
        resp = store.get_online_features(
            features=[f"{FV_NAMES[0]}:metric_a", f"{FV_NAMES[0]}:metric_b"],
            entity_rows=[{"entity_id": i} for i in range(1, 6)],
            full_feature_names=True,
        ).to_dict()
        print(resp)
        vals = [v for k, vs in resp.items() if k != "entity_id" for v in vs]
        non_null = sum(1 for v in vals if v is not None)
        print(f"non_null={non_null}/{len(vals)}")
        if non_null == 0:
            print("FRICTION: all online values null")
            return 3
        print("PASS: online features have values")
    except Exception as e:
        print(f"FRICTION/FAIL online: {type(e).__name__}: {e}")
        return 4

    print("\n=== ALL CLIENT STEPS DONE ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
