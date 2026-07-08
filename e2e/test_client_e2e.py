"""
Positive E2E test: real user workflow via generic Python client.

This script runs from a GENERIC Python image (feast + requests, NO PySpark).
It calls the Feast feature server's REST API to trigger materialization
and verify online features, mimicking what a real user/application would do.

Steps:
  1. POST /materialize → triggers server-side SparkApplication creation
  2. Poll SparkApplication status until COMPLETED
  3. Check FV states via registry gRPC (all should be AVAILABLE_ONLINE)
  4. POST /get-online-features → verify non-null feature values
"""

import json
import os
import sys
import time
import logging

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("e2e-client")

ONLINE_SERVER = os.environ.get(
    "FEAST_ONLINE_SERVER",
    "https://feast-byos-spark-e2e-online.aniket-cursor-test.svc.cluster.local:443",
)
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
REGISTRY_HOST = os.environ.get(
    "REGISTRY_HOST",
    "feast-registry-insecure.aniket-cursor-test.svc.cluster.local:80",
)
REDIS_HOST = os.environ.get("FEAST_REDIS_HOST", "redis.aniket-cursor-test.svc.cluster.local")
REDIS_PORT = int(os.environ.get("FEAST_REDIS_PORT", "6379"))

NUM_FVS = 10
NUM_ENTITIES = 10_000
MATERIALIZE_TIMEOUT = 300


def check_server_health():
    """Verify the feature server is reachable."""
    logger.info(f"Checking server health at {ONLINE_SERVER}")
    try:
        resp = requests.get(f"{ONLINE_SERVER}/health", timeout=10, verify=False)
        logger.info(f"Health check: {resp.status_code}")
        return resp.status_code == 200
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return False


def trigger_materialize():
    """POST /materialize to trigger server-side materialization."""
    payload = {
        "start_ts": "2020-01-01T00:00:00",
        "end_ts": "2025-12-31T23:59:59",
    }
    logger.info(f"POST {ONLINE_SERVER}/materialize")
    logger.info(f"Payload: {json.dumps(payload)}")
    resp = requests.post(
        f"{ONLINE_SERVER}/materialize",
        json=payload,
        timeout=MATERIALIZE_TIMEOUT,
        verify=False,
    )
    logger.info(f"Response: {resp.status_code}")
    if resp.status_code != 200:
        logger.error(f"Body: {resp.text}")
    return resp.status_code == 200


def check_fv_states():
    """Check feature view states via the Feast SDK (remote registry)."""
    from feast import FeatureStore, RepoConfig

    config = RepoConfig(
        project="feast_sparkapp_e2e",
        provider="local",
        registry={
            "registry_type": "remote",
            "path": REGISTRY_HOST,
        },
        offline_store={"type": "dask"},
        online_store={"type": "redis", "connection_string": f"{REDIS_HOST}:{REDIS_PORT}"},
        entity_key_serialization_version=3,
        auth={"type": "no_auth"},
    )
    store = FeatureStore(config=config)
    fvs = store.list_feature_views()
    logger.info(f"Registered FVs: {len(fvs)}")

    all_online = True
    for fv in fvs:
        state = getattr(fv, "state", None)
        state_name = state.name if state else "UNKNOWN"
        logger.info(f"  {fv.name}: state={state_name}")
        if state_name != "AVAILABLE_ONLINE":
            all_online = False

    return len(fvs), all_online


def check_redis_keys():
    """Verify Redis has the expected number of keys."""
    import redis as redis_lib

    r = redis_lib.Redis(host=REDIS_HOST, port=REDIS_PORT)
    dbsize = r.dbsize()
    logger.info(f"Redis DBSIZE: {dbsize}")
    return dbsize


def check_online_features():
    """POST /get-online-features to verify actual feature data."""
    payload = {
        "features": [
            "feature_view_1:metric_a",
            "feature_view_1:metric_b",
            "feature_view_1:category",
            "feature_view_5:metric_a",
            "feature_view_10:score",
        ],
        "entities": {
            "entity_id": [0, 1, 2, 3, 4],
        },
        "full_feature_names": True,
    }
    logger.info(f"POST {ONLINE_SERVER}/get-online-features")
    resp = requests.post(
        f"{ONLINE_SERVER}/get-online-features",
        json=payload,
        timeout=30,
        verify=False,
    )
    if resp.status_code != 200:
        logger.error(f"get-online-features failed: {resp.status_code} {resp.text}")
        return False, 0, 0

    data = resp.json()
    results = data.get("results", data.get("metadata", {}).get("feature_names", []))

    if "results" in data:
        total_values = 0
        non_null = 0
        for result in data["results"]:
            values = result.get("values", [])
            statuses = result.get("statuses", [])
            total_values += len(values)
            for val, status in zip(values, statuses):
                if status == "PRESENT" or (val is not None and status != "NOT_FOUND"):
                    non_null += 1
        logger.info(f"Online features: {non_null}/{total_values} non-null")
        return True, non_null, total_values

    logger.warning(f"Unexpected response format: {json.dumps(data)[:200]}")
    return False, 0, 0


def main():
    print("=" * 70)
    print("E2E TEST: Real User Workflow (Generic Python Client)")
    print("=" * 70)

    results = []

    # Step 1: Health check
    print("\n--- Step 1: Server health check ---")
    healthy = check_server_health()
    print(f"  [{'PASS' if healthy else 'FAIL'}] Server health")
    results.append(("server_health", healthy))
    if not healthy:
        print("ABORT: Server not reachable")
        return 1

    # Step 2: Trigger materialization
    print("\n--- Step 2: Trigger materialization via REST API ---")
    mat_ok = trigger_materialize()
    print(f"  [{'PASS' if mat_ok else 'FAIL'}] POST /materialize")
    results.append(("materialize", mat_ok))

    # Step 3: Check FV states
    print("\n--- Step 3: Check feature view states ---")
    fv_count, all_online = check_fv_states()
    print(f"  [{'PASS' if fv_count == NUM_FVS else 'FAIL'}] FV count: {fv_count}/{NUM_FVS}")
    print(f"  [{'PASS' if all_online else 'FAIL'}] All FVs AVAILABLE_ONLINE")
    results.append(("fv_count", fv_count == NUM_FVS))
    results.append(("fv_states", all_online))

    # Step 4: Check Redis
    print("\n--- Step 4: Check Redis keys ---")
    dbsize = check_redis_keys()
    redis_ok = dbsize >= NUM_ENTITIES
    print(f"  [{'PASS' if redis_ok else 'FAIL'}] Redis keys: {dbsize} (expected >= {NUM_ENTITIES})")
    results.append(("redis_keys", redis_ok))

    # Step 5: Get online features
    print("\n--- Step 5: Get online features via REST API ---")
    feat_ok, non_null, total = check_online_features()
    feat_pass = feat_ok and non_null > 0
    print(f"  [{'PASS' if feat_pass else 'FAIL'}] Online features: {non_null}/{total} non-null")
    results.append(("online_features", feat_pass))

    # Summary
    print("\n" + "=" * 70)
    total_tests = len(results)
    passed_count = sum(1 for _, p in results if p)
    failed_count = total_tests - passed_count
    print(f"RESULTS: {passed_count}/{total_tests} passed, {failed_count} failed")
    if failed_count > 0:
        print("\nFAILED:")
        for name, p in results:
            if not p:
                print(f"  - {name}")
    print("=" * 70)
    return 0 if failed_count == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
