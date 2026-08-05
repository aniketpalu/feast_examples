#!/usr/bin/env python3
"""Upload feature_repo/data/city_embeddings.parquet to MinIO bucket feast-rag."""

from __future__ import annotations

import os
import sys
from pathlib import Path

try:
    import boto3
except ImportError:
    print("pip install boto3", file=sys.stderr)
    sys.exit(1)

BUCKET = "feast-rag"
KEY = "city_embeddings.parquet"
REPO_PARQUET = Path(__file__).resolve().parents[2] / "feature_repo" / "data" / KEY


def main() -> None:
    endpoint = os.environ.get("AWS_ENDPOINT_URL_S3", "http://minio:9000")
    access = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
    secret = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")
    src = Path(os.environ.get("PARQUET_PATH", str(REPO_PARQUET)))
    if not src.exists():
        raise SystemExit(f"missing parquet: {src}")

    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access,
        aws_secret_access_key=secret,
        region_name="us-east-1",
    )
    try:
        client.head_bucket(Bucket=BUCKET)
    except Exception:
        client.create_bucket(Bucket=BUCKET)
        print(f"created s3://{BUCKET}")
    client.upload_file(str(src), BUCKET, KEY)
    print(f"uploaded s3://{BUCKET}/{KEY} ({src.stat().st_size} bytes) via {endpoint}")


if __name__ == "__main__":
    main()
