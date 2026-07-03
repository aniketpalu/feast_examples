"""Feature view and entity definitions for the large-scale E2E test."""

from datetime import timedelta

from feast import Entity, FeatureView, Field
from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import (
    SparkSource,
)
from feast.types import Float64, Int64, String

NUM_FEATURE_VIEWS = 10
MINIO_BUCKET = "feast-spark-e2e"

entity = Entity(name="entity_id", join_keys=["entity_id"])

feature_views = []
for i in range(1, NUM_FEATURE_VIEWS + 1):
    source = SparkSource(
        name=f"source_{i}",
        path=f"s3a://{MINIO_BUCKET}/data/fv_{i}.parquet",
        file_format="parquet",
        timestamp_field="event_timestamp",
    )
    fv = FeatureView(
        name=f"feature_view_{i}",
        entities=[entity],
        schema=[
            Field(name="metric_a", dtype=Float64),
            Field(name="metric_b", dtype=Float64),
            Field(name="metric_c", dtype=Float64),
            Field(name="category", dtype=String),
            Field(name="score", dtype=Int64),
        ],
        source=source,
        ttl=timedelta(days=365),
    )
    feature_views.append(fv)
