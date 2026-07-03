"""
E2E test for SparkApplicationComputeEngine with per-FV result reporting.

Materializes 10 feature views (100,000 rows each, 1M total) via
SparkApplication with 5 executors on OpenShift (ODH Spark Operator).

Prerequisites:
  - setup_data.py has been run (data in MinIO, FVs registered)
  - Port-forwards active: registry(6570), redis(6379), minio(9000)
"""

import os
import sys
import logging
import time
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s"
)
logger = logging.getLogger("e2e-test")

os.environ["AWS_ACCESS_KEY_ID"] = "minioadmin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "minioadmin"
os.environ["AWS_ENDPOINT_URL"] = "http://localhost:9000"
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from feast import FeatureStore, RepoConfig

NAMESPACE = "aniket-cursor-test"
MINIO_INTERNAL = "http://minio.aniket-cursor-test.svc.cluster.local:9000"
REDIS_INTERNAL = "redis.aniket-cursor-test.svc.cluster.local:6379"
REGISTRY_INTERNAL = (
    "feast-byos-spark-e2e-registry.aniket-cursor-test.svc.cluster.local:80"
)
SPARK_DRIVER_IMAGE = (
    "quay.io/aniket-redhat/feast-spark-driver:byos-spark-dev-0.9"
)

S3A_CONF = {
    "spark.hadoop.fs.s3a.endpoint": MINIO_INTERNAL,
    "spark.hadoop.fs.s3a.access.key": "minioadmin",
    "spark.hadoop.fs.s3a.secret.key": "minioadmin",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
}

NUM_FVS = 10
FV_NAMES = [f"feature_view_{i}" for i in range(1, NUM_FVS + 1)]


def make_materialize_config():
    """Config for materialization — online_store uses cluster-internal Redis."""
    return RepoConfig(
        project="feast_sparkapp_e2e",
        provider="local",
        registry={"registry_type": "remote", "path": "localhost:6570"},
        offline_store={"type": "spark", "spark_conf": S3A_CONF},
        online_store={"type": "redis", "connection_string": REDIS_INTERNAL},
        batch_engine={
            "type": "spark_application",
            "image": SPARK_DRIVER_IMAGE,
            "namespace": NAMESPACE,
            "service_account": "feast-spark-driver",
            "executor_instances": 5,
            "executor_cores": 1,
            "executor_memory": "2g",
            "driver_cores": 1,
            "driver_memory": "2g",
            "concurrency": 10,
            "registry_address": REGISTRY_INTERNAL,
            "spark_conf": S3A_CONF,
        },
        entity_key_serialization_version=3,
        auth={"type": "no_auth"},
    )


def make_verify_config():
    """Config for verification — online_store uses localhost port-forward."""
    return RepoConfig(
        project="feast_sparkapp_e2e",
        provider="local",
        registry={"registry_type": "remote", "path": "localhost:6570"},
        offline_store={"type": "spark", "spark_conf": S3A_CONF},
        online_store={"type": "redis", "connection_string": "localhost:6379"},
        entity_key_serialization_version=3,
        auth={"type": "no_auth"},
    )


def print_fv_states(store, label):
    logger.info(f"--- {label} ---")
    for fv in store.list_feature_views():
        if fv.name.startswith("feature_view_"):
            state = getattr(fv, "state", "N/A")
            state_name = state.name if hasattr(state, "name") else str(state)
            logger.info(
                f"  {fv.name}: state={state.value} ({state_name}), "
                f"most_recent_end_time={fv.most_recent_end_time}"
            )


def run_materialization(store):
    logger.info("=== Starting materialization ===")
    logger.info(
        f"  {NUM_FVS} feature views, 100K rows each, "
        f"5 executors, concurrency=10"
    )
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end = datetime(2024, 1, 2, tzinfo=timezone.utc)

    t0 = time.time()
    store.materialize(
        start_date=start,
        end_date=end,
        feature_views=FV_NAMES,
    )
    elapsed = time.time() - t0
    logger.info(f"=== Materialization completed in {elapsed:.1f}s ===")
    return elapsed


def verify_online_features(store):
    logger.info("=== Verifying online features ===")
    entity_rows = [{"entity_id": i} for i in [0, 500, 5000, 9999]]

    features = store.get_online_features(
        features=[f"feature_view_{i}:metric_a" for i in range(1, NUM_FVS + 1)],
        entity_rows=entity_rows,
        full_feature_names=True,
    )
    result = features.to_dict()
    for key, values in result.items():
        logger.info(f"  {key}: {values}")

    non_null = sum(
        1
        for k, v in result.items()
        if k != "entity_id"
        for val in v
        if val is not None
    )
    total = sum(1 for k, v in result.items() if k != "entity_id" for _ in v)
    logger.info(f"  Non-null values: {non_null}/{total}")
    return result


def main():
    logger.info("=" * 60)
    logger.info("SparkApplicationComputeEngine E2E Test")
    logger.info(f"  Scale: {NUM_FVS} FVs x 100,000 rows = 1,000,000 total")
    logger.info(f"  Executors: 5, Concurrency: 10")
    logger.info(f"  Image: {SPARK_DRIVER_IMAGE}")
    logger.info("=" * 60)

    mat_store = FeatureStore(config=make_materialize_config())

    print_fv_states(mat_store, "Pre-materialization FV states")

    try:
        elapsed = run_materialization(mat_store)
    except Exception as e:
        logger.exception(f"Materialization FAILED: {e}")
        sys.exit(1)

    print_fv_states(mat_store, "Post-materialization FV states")

    verify_store = FeatureStore(config=make_verify_config())
    try:
        verify_online_features(verify_store)
    except Exception as e:
        logger.exception(f"Online feature verification failed: {e}")

    logger.info("=" * 60)
    logger.info(f"E2E TEST PASSED — {elapsed:.1f}s total")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
