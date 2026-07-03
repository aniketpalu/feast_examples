"""
Generate large-scale test data and register feature views.

Creates 10 parquet files with 100,000 rows each (1M total) in MinIO,
then registers all entities and feature views in the Feast registry.

Run this inside the cluster (e.g., via setup-pod.yaml) so it can
reach MinIO and the registry directly.
"""

import os
import sys
import logging
import tempfile
import time

import boto3
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger("setup")

MINIO_ENDPOINT = os.environ.get(
    "MINIO_ENDPOINT", "http://minio.aniket-cursor-test.svc.cluster.local:9000"
)
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
BUCKET = "feast-spark-e2e"
REGISTRY_HOST = os.environ.get(
    "REGISTRY_HOST",
    "feast-byos-spark-e2e-registry.aniket-cursor-test.svc.cluster.local:80",
)
NUM_FVS = 10
ROWS_PER_FV = 100_000
NUM_ENTITIES = 10_000
CATEGORIES = ["electronics", "clothing", "food", "automotive", "software",
              "healthcare", "finance", "education", "sports", "travel"]


def create_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )


def ensure_bucket(s3):
    try:
        s3.head_bucket(Bucket=BUCKET)
        logger.info(f"Bucket '{BUCKET}' exists")
    except Exception:
        s3.create_bucket(Bucket=BUCKET)
        logger.info(f"Created bucket '{BUCKET}'")


def generate_and_upload(s3, fv_index: int):
    t0 = time.time()
    np.random.seed(fv_index * 42)

    timestamps_per_entity = ROWS_PER_FV // NUM_ENTITIES
    base_ts = pd.Timestamp("2024-01-01", tz="UTC")

    entity_ids = np.repeat(np.arange(NUM_ENTITIES), timestamps_per_entity)
    timestamps = np.tile(
        pd.date_range(base_ts, periods=timestamps_per_entity, freq="h"),
        NUM_ENTITIES,
    )

    df = pd.DataFrame({
        "entity_id": entity_ids,
        "event_timestamp": timestamps,
        "metric_a": np.random.random(ROWS_PER_FV),
        "metric_b": np.random.random(ROWS_PER_FV) * 100,
        "metric_c": np.random.random(ROWS_PER_FV) * 1000,
        "category": np.random.choice(CATEGORIES, ROWS_PER_FV),
        "score": np.random.randint(0, 1000, ROWS_PER_FV),
    })

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        table = pa.Table.from_pandas(df)
        pq.write_table(table, f.name, row_group_size=50_000)
        tmp_path = f.name

    key = f"data/fv_{fv_index}.parquet"
    s3.upload_file(tmp_path, BUCKET, key)
    os.unlink(tmp_path)

    elapsed = time.time() - t0
    size_mb = os.path.getsize(tmp_path) if os.path.exists(tmp_path) else 0
    logger.info(
        f"FV {fv_index}: {ROWS_PER_FV:,} rows, "
        f"{NUM_ENTITIES:,} entities, uploaded to s3://{BUCKET}/{key} ({elapsed:.1f}s)"
    )


def register_feature_views():
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

    from feast import FeatureStore, RepoConfig

    config = RepoConfig(
        project="feast_sparkapp_e2e",
        provider="local",
        registry={
            "registry_type": "remote",
            "path": REGISTRY_HOST,
        },
        offline_store={
            "type": "spark",
            "spark_conf": {
                "spark.hadoop.fs.s3a.endpoint": MINIO_ENDPOINT,
                "spark.hadoop.fs.s3a.access.key": MINIO_ACCESS_KEY,
                "spark.hadoop.fs.s3a.secret.key": MINIO_SECRET_KEY,
                "spark.hadoop.fs.s3a.path.style.access": "true",
                "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            },
        },
        online_store={
            "type": "redis",
            "connection_string": "redis.aniket-cursor-test.svc.cluster.local:6379",
        },
        entity_key_serialization_version=3,
        auth={"type": "no_auth"},
    )

    store = FeatureStore(config=config)

    from definitions import entity, feature_views

    store.apply([entity] + feature_views)
    logger.info(f"Registered {len(feature_views)} feature views in registry")

    for fv in store.list_feature_views():
        logger.info(f"  {fv.name}: state={getattr(fv, 'state', 'N/A')}")


def main():
    logger.info(f"=== Generating {NUM_FVS} datasets x {ROWS_PER_FV:,} rows ===")
    logger.info(f"Total: {NUM_FVS * ROWS_PER_FV:,} rows, {NUM_ENTITIES:,} entities")

    s3 = create_s3_client()
    ensure_bucket(s3)

    total_t0 = time.time()
    for i in range(1, NUM_FVS + 1):
        generate_and_upload(s3, i)
    logger.info(f"Data generation complete in {time.time() - total_t0:.1f}s")

    logger.info("=== Registering feature views ===")
    register_feature_views()

    logger.info("=== Setup complete ===")


if __name__ == "__main__":
    main()
