from datetime import timedelta

import dill

import pandas as pd
from feast import Entity, FeatureView, Field
from feast.batch_feature_view import BatchFeatureView
from feast.on_demand_feature_view import on_demand_feature_view
from feast.infra.offline_stores.contrib.postgres_offline_store.postgres_source import (
    PostgreSQLSource,
)
from feast.types import Float64, String

entity = Entity(name="entity_id", join_keys=["entity_id"])

FEATURE_VIEWS = []
for i in range(1, 11):
    source = PostgreSQLSource(
        name=f"fv_{i}_source",
        table=f"fv_{i}",
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
            Field(name="score", dtype=Float64),
        ],
        source=source,
        ttl=timedelta(days=3650),
    )
    FEATURE_VIEWS.append(fv)

feature_view_1 = FEATURE_VIEWS[0]
feature_view_2 = FEATURE_VIEWS[1]
feature_view_3 = FEATURE_VIEWS[2]
feature_view_4 = FEATURE_VIEWS[3]
feature_view_5 = FEATURE_VIEWS[4]
feature_view_6 = FEATURE_VIEWS[5]
feature_view_7 = FEATURE_VIEWS[6]
feature_view_8 = FEATURE_VIEWS[7]
feature_view_9 = FEATURE_VIEWS[8]
feature_view_10 = FEATURE_VIEWS[9]


# Small ODFV: sum of materialized metrics from feature_view_1 (computed at request time).
@on_demand_feature_view(
    sources=[feature_view_1],
    schema=[Field(name="metric_sum", dtype=Float64)],
)
def metric_sum_odfv(inputs: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    df["metric_sum"] = inputs["metric_a"] + inputs["metric_b"]
    return df


# BatchFeatureView UDF for SparkApplication materialize (E2E-3 / RHOAIENG-57664).
# SparkTransformationNode passes a Spark DataFrame — use Column ops, not pandas.
# Avoid `from pyspark.sql import functions as F` inside the UDF: dill-deserialized
# nested pyspark imports segfault on Spark 4.0.1 / this driver image (exit 139).
# IMPORTANT: dill bytecode is Python-version-specific. feast-apply (feature-server)
# and the Spark driver image must use the same Python major.minor, OR re-apply this
# view from a process that matches the driver (see e2e/apply_udf_bfv_driver_job.yaml).
#
# Requires Postgres offline store to treat empty feature_cols as SELECT * (Feast
# signals that for mode=python transformations). See driver image patch /
# feast postgres pull_latest empty-cols fix.
def double_metrics(df):
    """Double metric_a; set metric_b = doubled_a + original_b (PySpark Columns)."""
    df = df.withColumn("metric_a", df["metric_a"] * 2.0)
    df = df.withColumn("metric_b", df["metric_a"] + df["metric_b"])
    return df


# dill must not require a same-named module on the registry server / driver.
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
        Field(name="score", dtype=Float64),
    ],
    source=PostgreSQLSource(
        name="udf_double_metrics_source",
        table="fv_1",
        timestamp_field="event_timestamp",
    ),
    udf=double_metrics,
    # Required so SparkTransformationNode can re-exec instead of calling
    # dill-deserialized bytecode (segfaults on Spark 4.0.1 DataFrame ops).
    udf_string=dill.source.getsource(double_metrics),
    online=True,
)

