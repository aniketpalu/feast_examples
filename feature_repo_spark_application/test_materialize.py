"""
Post-materialization verification script.

Run inside the cluster (e.g., via kubectl exec or a test pod) to verify
that materialization completed successfully by checking:
1. Feature view states in the registry
2. Online store (Redis) key count
3. Sample feature retrieval via get_online_features
"""

import os
import sys
import logging

import redis

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger("test")

REDIS_HOST = os.environ.get("REDIS_HOST", "redis.aniket-cursor-test.svc.cluster.local")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
REGISTRY_HOST = os.environ.get(
    "REGISTRY_HOST",
    "feast-byos-spark-e2e-registry.aniket-cursor-test.svc.cluster.local:80",
)
NUM_FVS = 10
NUM_ENTITIES = 10_000


def check_redis():
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
    dbsize = r.dbsize()
    logger.info(f"Redis DBSIZE: {dbsize}")
    assert dbsize >= NUM_ENTITIES, (
        f"Expected at least {NUM_ENTITIES} keys, got {dbsize}"
    )
    logger.info("PASS: Redis key count")


def check_feature_view_states():
    from feast import FeatureStore, RepoConfig

    config = RepoConfig(
        project="feast_sparkapp_e2e",
        provider="local",
        registry={
            "registry_type": "remote",
            "path": REGISTRY_HOST,
        },
        offline_store={"type": "dask"},
        online_store={
            "type": "redis",
            "connection_string": f"{REDIS_HOST}:{REDIS_PORT}",
        },
        entity_key_serialization_version=3,
        auth={"type": "no_auth"},
    )
    store = FeatureStore(config=config)

    fvs = store.list_feature_views()
    logger.info(f"Registered feature views: {len(fvs)}")
    assert len(fvs) == NUM_FVS, f"Expected {NUM_FVS} FVs, got {len(fvs)}"

    for fv in fvs:
        state = getattr(fv, "state", None)
        state_name = state.name if state else "UNKNOWN"
        logger.info(f"  {fv.name}: state={state_name}")
        assert state_name == "AVAILABLE_ONLINE", (
            f"{fv.name} state is {state_name}, expected AVAILABLE_ONLINE"
        )
    logger.info("PASS: All feature view states are AVAILABLE_ONLINE")


def check_online_features():
    from feast import FeatureStore, RepoConfig

    config = RepoConfig(
        project="feast_sparkapp_e2e",
        provider="local",
        registry={
            "registry_type": "remote",
            "path": REGISTRY_HOST,
        },
        offline_store={"type": "dask"},
        online_store={
            "type": "redis",
            "connection_string": f"{REDIS_HOST}:{REDIS_PORT}",
        },
        entity_key_serialization_version=3,
        auth={"type": "no_auth"},
    )
    store = FeatureStore(config=config)

    entity_rows = [{"entity_id": i} for i in range(5)]
    features = [
        "feature_view_1:metric_a",
        "feature_view_1:metric_b",
        "feature_view_1:category",
    ]
    result = store.get_online_features(features=features, entity_rows=entity_rows)
    df = result.to_df()
    logger.info(f"Online feature sample (5 rows):\n{df}")

    assert len(df) == 5, f"Expected 5 rows, got {len(df)}"
    assert df["metric_a"].notna().all(), "metric_a has NaN values"
    logger.info("PASS: Online feature retrieval")


def main():
    logger.info("=== Post-materialization verification ===")

    logger.info("--- Check 1: Redis key count ---")
    check_redis()

    logger.info("--- Check 2: Feature view states ---")
    check_feature_view_states()

    logger.info("--- Check 3: Online feature retrieval ---")
    check_online_features()

    logger.info("=== ALL CHECKS PASSED ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
