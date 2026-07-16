"""E2E: remote-materialize BatchFeatureView UDF via SparkApplication; verify online.

Preconditions:
  - FeatureStore spark-pg-e2e Ready with batchEngine
  - udf_double_metrics registered with driver-matching Python (see apply_udf_bfv*)
  - feature_view_1 already materialized (for baseline raw values) OR Postgres reachable

Run in notebook:
  PYTHONPATH=/tmp/feast_merged_sdk python3 /tmp/notebook_udf_bfv_test.py
"""

from __future__ import annotations

import os
import time
from datetime import datetime, timezone

from feast import FeatureStore

REPO = "/tmp/spark-pg-e2e-client-udf"
CERT = "/var/run/secrets/kubernetes.io/serviceaccount/service-ca.crt"
UDF_FV = "udf_double_metrics"
BASE_FV = "feature_view_1"


def _write_yaml() -> None:
    os.makedirs(REPO, exist_ok=True)
    open(f"{REPO}/feature_store.yaml", "w").write(
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
"""
    )


def main() -> None:
    _write_yaml()
    store = FeatureStore(repo_path=REPO)
    names = sorted(fv.name for fv in store.list_feature_views())
    print(f"FVs ({len(names)}): {names}", flush=True)
    assert UDF_FV in names, f"{UDF_FV} missing — run apply_udf_bfv_driver_job first"

    entities = [{"entity_id": i} for i in range(1, 6)]

    print("=== baseline online from feature_view_1 ===", flush=True)
    base = store.get_online_features(
        features=[f"{BASE_FV}:metric_a", f"{BASE_FV}:metric_b"],
        entity_rows=entities,
    ).to_dict()
    print(base, flush=True)

    print(f"=== remote materialize {UDF_FV} via SparkApplication ===", flush=True)
    t0 = time.time()
    store.materialize(
        datetime(2024, 1, 1, tzinfo=timezone.utc),
        datetime(2024, 1, 15, tzinfo=timezone.utc),
        feature_views=[UDF_FV],
        remote=True,
        wait=True,
        timeout=900,
        poll_interval=3,
    )
    print(f"PASS: materialize returned in {time.time() - t0:.1f}s", flush=True)

    def _read_udf():
        return store.get_online_features(
            features=[f"{UDF_FV}:metric_a", f"{UDF_FV}:metric_b"],
            entity_rows=entities,
        ).to_dict()

    udf = None
    for attempt in range(1, 6):
        try:
            udf = _read_udf()
            if all(v is not None for v in udf["metric_a"]):
                break
        except Exception as e:
            print(f"online attempt {attempt} error: {e}", flush=True)
        time.sleep(3)
    assert udf is not None
    print("UDF online:", udf, flush=True)

    ok = 0
    for i in range(len(entities)):
        raw_a, raw_b = base["metric_a"][i], base["metric_b"][i]
        got_a, got_b = udf["metric_a"][i], udf["metric_b"][i]
        assert raw_a is not None and raw_b is not None, f"missing baseline entity {entities[i]}"
        assert got_a is not None and got_b is not None, f"missing udf entity {entities[i]}"
        exp_a = raw_a * 2.0
        exp_b = exp_a + raw_b
        assert abs(got_a - exp_a) < 1e-6, f"metric_a entity {entities[i]['entity_id']}: {got_a} != {exp_a}"
        assert abs(got_b - exp_b) < 1e-6, f"metric_b entity {entities[i]['entity_id']}: {got_b} != {exp_b}"
        ok += 1
        print(
            f"entity {entities[i]['entity_id']}: raw_a={raw_a:.4f} -> udf_a={got_a:.4f} "
            f"udf_b={got_b:.4f} (expect {exp_b:.4f})",
            flush=True,
        )
    print(f"PASS: UDF online {ok}/{len(entities)} entities match double_metrics", flush=True)


if __name__ == "__main__":
    main()
