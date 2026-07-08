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


def _make_fv(i: int) -> FeatureView:
    source = SparkSource(
        name=f"source_{i}",
        path=f"s3a://{MINIO_BUCKET}/data/fv_{i}.parquet",
        file_format="parquet",
        timestamp_field="event_timestamp",
    )
    return FeatureView(
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


feature_view_1 = _make_fv(1)
feature_view_2 = _make_fv(2)
feature_view_3 = _make_fv(3)
feature_view_4 = _make_fv(4)
feature_view_5 = _make_fv(5)
feature_view_6 = _make_fv(6)
feature_view_7 = _make_fv(7)
feature_view_8 = _make_fv(8)
feature_view_9 = _make_fv(9)
feature_view_10 = _make_fv(10)
