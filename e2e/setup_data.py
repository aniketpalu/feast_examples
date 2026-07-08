"""Generate test data and upload to MinIO. Run from any Python pod with boto3+pandas."""

import os
import sys
import logging
import tempfile

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("setup")

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio.aniket-cursor-test.svc.cluster.local:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
BUCKET = "feast-spark-e2e"
NUM_FVS = 10
ROWS_PER_FV = 100_000
NUM_ENTITIES = 10_000
CATEGORIES = ["electronics", "clothing", "food", "automotive", "software",
              "healthcare", "finance", "education", "sports", "travel"]


def main():
    import boto3
    import numpy as np
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq

    s3 = boto3.client("s3", endpoint_url=MINIO_ENDPOINT,
                      aws_access_key_id=MINIO_ACCESS_KEY,
                      aws_secret_access_key=MINIO_SECRET_KEY,
                      region_name="us-east-1")

    try:
        s3.head_bucket(Bucket=BUCKET)
    except Exception:
        s3.create_bucket(Bucket=BUCKET)
        logger.info(f"Created bucket '{BUCKET}'")

    logger.info(f"Generating {NUM_FVS} datasets x {ROWS_PER_FV:,} rows = {NUM_FVS * ROWS_PER_FV:,} total")

    for i in range(1, NUM_FVS + 1):
        np.random.seed(i * 42)
        ts_per_entity = ROWS_PER_FV // NUM_ENTITIES
        base_ts = pd.Timestamp("2024-01-01", tz="UTC")
        entity_ids = np.repeat(np.arange(NUM_ENTITIES), ts_per_entity)
        timestamps = np.tile(pd.date_range(base_ts, periods=ts_per_entity, freq="h"), NUM_ENTITIES)

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
            pq.write_table(pa.Table.from_pandas(df), f.name, row_group_size=50_000)
            tmp_path = f.name

        key = f"data/fv_{i}.parquet"
        s3.upload_file(tmp_path, BUCKET, key)
        os.unlink(tmp_path)
        logger.info(f"  FV {i}: {ROWS_PER_FV:,} rows → s3://{BUCKET}/{key}")

    logger.info("Data generation complete")


if __name__ == "__main__":
    main()
