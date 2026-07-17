"""SparkSource + BatchFeatureView UDF for SparkApplication E2E (non-Postgres offline)."""

from datetime import timedelta

import dill

from feast import Entity, FeatureView, Field
from feast.batch_feature_view import BatchFeatureView
from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import (
    SparkSource,
)
from feast.types import Float64, Int64, String

MINIO_BUCKET = "feast-spark-e2e"

entity = Entity(name="entity_id", join_keys=["entity_id"])

_source_1 = SparkSource(
    name="source_1",
    path=f"s3a://{MINIO_BUCKET}/data/fv_1.parquet",
    file_format="parquet",
    timestamp_field="event_timestamp",
)

feature_view_1 = FeatureView(
    name="feature_view_1",
    entities=[entity],
    schema=[
        Field(name="metric_a", dtype=Float64),
        Field(name="metric_b", dtype=Float64),
        Field(name="metric_c", dtype=Float64),
        Field(name="category", dtype=String),
        Field(name="score", dtype=Int64),
    ],
    source=_source_1,
    ttl=timedelta(days=3650),
)


def double_metrics(df):
    """Double metric_a; metric_b = doubled_a + original_b (PySpark Columns)."""
    df = df.withColumn("metric_a", df["metric_a"] * 2.0)
    df = df.withColumn("metric_b", df["metric_a"] + df["metric_b"])
    return df


_DOUBLE_METRICS_SRC = dill.source.getsource(double_metrics)

if double_metrics.__module__ != "__main__":
    double_metrics.__module__ = "__main__"


udf_double_metrics = BatchFeatureView(
    name="udf_double_metrics",
    mode="python",
    entities=[entity],
    ttl=timedelta(days=3650),
    schema=[
        Field(name="metric_a", dtype=Float64),
        Field(name="metric_b", dtype=Float64),
        Field(name="metric_c", dtype=Float64),
        Field(name="category", dtype=String),
        Field(name="score", dtype=Int64),
    ],
    source=SparkSource(
        name="udf_double_metrics_source",
        path=f"s3a://{MINIO_BUCKET}/data/fv_1.parquet",
        file_format="parquet",
        timestamp_field="event_timestamp",
    ),
    udf=double_metrics,
    udf_string=_DOUBLE_METRICS_SRC,
    online=True,
)
