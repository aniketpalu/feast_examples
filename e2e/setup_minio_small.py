"""Upload small fv_1.parquet to MinIO for SparkSource UDF E2E."""

from __future__ import annotations

import os
import tempfile
import time

import boto3
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
BUCKET = os.environ.get("MINIO_BUCKET", "feast-spark-e2e")
NUM_ENTITIES = int(os.environ.get("NUM_ENTITIES", "100"))
ROWS_PER_ENTITY = int(os.environ.get("ROWS_PER_ENTITY", "10"))


def main() -> None:
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )
    try:
        s3.head_bucket(Bucket=BUCKET)
    except Exception:
        s3.create_bucket(Bucket=BUCKET)
        print(f"created bucket {BUCKET}", flush=True)

    n = NUM_ENTITIES * ROWS_PER_ENTITY
    np.random.seed(1)
    base_ts = pd.Timestamp("2024-01-01", tz="UTC")
    entity_ids = np.repeat(np.arange(1, NUM_ENTITIES + 1), ROWS_PER_ENTITY)
    timestamps = np.tile(
        pd.date_range(base_ts, periods=ROWS_PER_ENTITY, freq="h"),
        NUM_ENTITIES,
    )
    # Deterministic metrics for entities 1..5 assertions
    metric_a = np.random.random(n)
    metric_b = np.random.random(n) * 100
    for i in range(1, 6):
        idx = (entity_ids == i) & (timestamps == timestamps[entity_ids == i].max())
        metric_a[idx] = float(i) + 0.1
        metric_b[idx] = float(i) * 10.0

    df = pd.DataFrame(
        {
            "entity_id": entity_ids.astype(np.int64),
            "event_timestamp": timestamps,
            "metric_a": metric_a,
            "metric_b": metric_b,
            "metric_c": np.random.random(n) * 1000,
            "category": np.random.choice(["a", "b", "c"], n),
            "score": np.random.randint(0, 1000, n, dtype=np.int64),
        }
    )
    # Spark/Parquet: microsecond timestamps
    df["event_timestamp"] = df["event_timestamp"].astype("datetime64[us, UTC]")

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        table = pa.Table.from_pandas(df)
        pq.write_table(table, f.name)
        path = f.name
    key = "data/fv_1.parquet"
    s3.upload_file(path, BUCKET, key)
    os.unlink(path)
    print(f"PASS: uploaded s3://{BUCKET}/{key} rows={n} entities={NUM_ENTITIES}", flush=True)


if __name__ == "__main__":
    main()
