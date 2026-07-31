#!/usr/bin/env python3
"""Prepare Spark-safe city embeddings parquet for Option A RAG demo.

Reads the upstream Feast examples/rag parquet (or --src), coerces timestamps
to microseconds UTC (Spark 4 rejects nanoseconds), and writes:

  feature_repo/data/city_embeddings.parquet

Optional: upload to MinIO when --upload is set (needs boto3 + env creds).

Usage:
  python prepare_data.py
  python prepare_data.py --src /path/to/city_wikipedia_summaries_with_embeddings.parquet
  python prepare_data.py --upload --endpoint http://localhost:9000
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

REPO_DIR = Path(__file__).resolve().parent
DEFAULT_OUT = REPO_DIR / "data" / "city_embeddings.parquet"
BUCKET = "feast-rag"
OBJECT_KEY = "city_embeddings.parquet"
COLS = [
    "item_id",
    "event_timestamp",
    "state",
    "wiki_summary",
    "sentence_chunks",
    "vector",
]


def to_spark_safe(df: pd.DataFrame) -> pa.Table:
    missing = [c for c in COLS if c not in df.columns]
    if missing:
        raise SystemExit(f"parquet missing columns: {missing}")
    df = df[COLS].copy()
    df["event_timestamp"] = pd.to_datetime(df["event_timestamp"], utc=True)
    table = pa.Table.from_pandas(df, preserve_index=False)
    fields = []
    for f in table.schema:
        if pa.types.is_timestamp(f.type) and f.type.unit != "us":
            fields.append(pa.field(f.name, pa.timestamp("us", tz="UTC")))
        else:
            fields.append(f)
    return table.cast(pa.schema(fields))


def write_parquet(table: pa.Table, out: Path) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, out, coerce_timestamps="us")
    dim = len(table.column("vector")[0].as_py())
    print(f"wrote {out} rows={table.num_rows} vector_dim={dim} bytes={out.stat().st_size}")


def upload_minio(path: Path, endpoint: str) -> None:
    try:
        import boto3
    except ImportError as e:
        raise SystemExit("boto3 required for --upload: pip install boto3") from e

    access = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
    secret = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")
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
        print(f"created bucket s3://{BUCKET}")
    client.upload_file(str(path), BUCKET, OBJECT_KEY)
    print(f"uploaded s3://{BUCKET}/{OBJECT_KEY} via {endpoint}")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--src",
        type=Path,
        default=None,
        help="Source parquet (default: reuse existing data/city_embeddings.parquet columns)",
    )
    p.add_argument("--out", type=Path, default=DEFAULT_OUT)
    p.add_argument("--upload", action="store_true", help="Upload to MinIO/S3")
    p.add_argument(
        "--endpoint",
        default=os.environ.get("AWS_ENDPOINT_URL_S3", "http://127.0.0.1:9000"),
        help="S3 endpoint for --upload",
    )
    args = p.parse_args()

    if args.src is not None:
        src = args.src
    elif DEFAULT_OUT.exists():
        # Re-normalize from checked-in sample
        src = DEFAULT_OUT
    else:
        raise SystemExit(
            "Provide --src path to Feast examples/rag city parquet, "
            "or keep feature_repo/data/city_embeddings.parquet in tree."
        )

    df = pd.read_parquet(src)
    table = to_spark_safe(df)
    write_parquet(table, args.out)
    if args.upload:
        upload_minio(args.out, args.endpoint)


if __name__ == "__main__":
    main()
