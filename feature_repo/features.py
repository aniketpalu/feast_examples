from datetime import timedelta
from feast import Entity, FeatureView, Field, ValueType
from feast.types import Int64, Float32, UnixTimestamp
from feast.infra.offline_stores.file_source import FileSource
from feast.data_format import ParquetFormat

# Entity: User
user_entity = Entity(
    name="user_id",
    description="User identifier",
    join_keys=["user_id"],
    value_type=ValueType.INT64,
)

# Data source - using Parquet file for Dask offline store
user_stats_source = FileSource(
    name="user_stats",
    path="data/user_stats.parquet",
    timestamp_field="created_timestamp",
    file_format=ParquetFormat(),
)

# Feature view for user statistics
user_stats_fv = FeatureView(
    name="user_stats",
    entities=[user_entity],
    ttl=timedelta(days=7),
    schema=[
        Field(name="avg_transaction_amount", dtype=Float32),
        Field(name="total_transactions", dtype=Int64),
        Field(name="last_active_date", dtype=UnixTimestamp),
    ],
    source=user_stats_source,
    online=True,
)

