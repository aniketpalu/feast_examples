from datetime import timedelta

from feast import Entity, FeatureView, Field
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
