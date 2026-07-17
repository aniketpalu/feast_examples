"""E2E: SparkSource BFV UDF via SparkApplication (v14); verify online."""

from __future__ import annotations

import os
import time
from datetime import datetime, timezone

from feast import FeatureStore

REPO = "/tmp/spark-src-udf-client"
CERT = "/var/run/secrets/kubernetes.io/serviceaccount/service-ca.crt"
UDF_FV = "udf_double_metrics"
BASE_FV = "feature_view_1"
NS = "feast-spark"
CR = "spark-src-udf"


def _write_yaml() -> None:
    os.makedirs(REPO, exist_ok=True)
    open(f"{REPO}/feature_store.yaml", "w").write(
        f"""project: feast_spark_src_udf
provider: local
registry:
  registry_type: remote
  path: feast-{CR}-registry.{NS}.svc.cluster.local:443
  cert: {CERT}
online_store:
  type: remote
  path: https://feast-{CR}-online.{NS}.svc.cluster.local:443
  cert: {CERT}
offline_store:
  type: remote
  host: feast-{CR}-offline.{NS}.svc.cluster.local
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
    assert UDF_FV in names, f"{UDF_FV} missing — run apply_udf_spark_source_driver_job"

    entities = [{"entity_id": i} for i in range(1, 6)]

    print(f"=== remote materialize {BASE_FV} ===", flush=True)
    store.materialize(
        datetime(2024, 1, 1, tzinfo=timezone.utc),
        datetime(2024, 1, 15, tzinfo=timezone.utc),
        feature_views=[BASE_FV],
        remote=True,
        wait=True,
        timeout=900,
        poll_interval=3,
    )

    base = None
    for attempt in range(1, 8):
        try:
            base = store.get_online_features(
                features=[f"{BASE_FV}:metric_a", f"{BASE_FV}:metric_b"],
                entity_rows=entities,
            ).to_dict()
            if all(v is not None for v in base["metric_a"]):
                break
        except Exception as e:
            print(f"baseline attempt {attempt}: {e}", flush=True)
        time.sleep(3)
    assert base is not None
    print("baseline:", base, flush=True)

    print(f"=== remote materialize {UDF_FV} ===", flush=True)
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
    print(f"PASS: UDF materialize in {time.time() - t0:.1f}s", flush=True)

    udf = None
    for attempt in range(1, 8):
        try:
            udf = store.get_online_features(
                features=[f"{UDF_FV}:metric_a", f"{UDF_FV}:metric_b"],
                entity_rows=entities,
            ).to_dict()
            if all(v is not None for v in udf["metric_a"]):
                break
        except Exception as e:
            print(f"udf online attempt {attempt}: {e}", flush=True)
        time.sleep(3)
    assert udf is not None
    print("UDF online:", udf, flush=True)

    ok = 0
    for i in range(len(entities)):
        raw_a, raw_b = base["metric_a"][i], base["metric_b"][i]
        got_a, got_b = udf["metric_a"][i], udf["metric_b"][i]
        assert raw_a is not None and got_a is not None
        exp_a = raw_a * 2.0
        exp_b = exp_a + raw_b
        assert abs(got_a - exp_a) < 1e-6, f"a[{i}]: {got_a} != {exp_a}"
        assert abs(got_b - exp_b) < 1e-6, f"b[{i}]: {got_b} != {exp_b}"
        ok += 1
        print(f"entity {entities[i]['entity_id']}: PASS a={got_a:.4f} b={got_b:.4f}", flush=True)
    print(f"PASS: SparkSource UDF online {ok}/{len(entities)}", flush=True)


if __name__ == "__main__":
    main()
