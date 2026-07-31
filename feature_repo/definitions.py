"""Option A — Feast RAG feature repo (pre-embedded cities → SparkApplication).

SparkApplication materializes existing 384-d vectors from MinIO into a vector
online store (Milvus). No embedding UDF — vectors are already in the parquet.

Source data: feast-dev/feast examples/rag city Wikipedia summaries (811 rows).
Upload path: see prepare_data.py → s3a://feast-rag/city_embeddings.parquet
"""

from datetime import timedelta

from feast import Entity, FeatureView, Field
from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import (
    SparkSource,
)
from feast.types import Array, Float32, String
from feast.value_type import ValueType

# MinIO / S3A bucket used by the SparkApplication offline store.
MINIO_BUCKET = "feast-rag"
PARQUET_PATH = f"s3a://{MINIO_BUCKET}/city_embeddings.parquet"

item = Entity(
    name="item_id",
    join_keys=["item_id"],
    value_type=ValueType.INT64,
    description="City Wikipedia chunk / document id for RAG retrieval",
)

city_embeddings_source = SparkSource(
    name="city_embeddings_source",
    path=PARQUET_PATH,
    file_format="parquet",
    timestamp_field="event_timestamp",
)

city_embeddings = FeatureView(
    name="city_embeddings",
    entities=[item],
    ttl=timedelta(days=3650),
    schema=[
        Field(
            name="vector",
            dtype=Array(Float32),
            vector_index=True,
            vector_search_metric="COSINE",
            description="384-d MiniLM embedding (pre-computed in parquet)",
        ),
        Field(name="state", dtype=String),
        Field(name="sentence_chunks", dtype=String),
        Field(name="wiki_summary", dtype=String),
    ],
    source=city_embeddings_source,
    online=True,
    tags={"use_case": "rag", "option": "A", "compute": "none"},
)
