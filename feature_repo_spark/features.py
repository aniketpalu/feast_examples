from datetime import timedelta
import os
from feast import Entity, FeatureView, Field, ValueType
from feast.types import Int64, Float32, UnixTimestamp
from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import SparkSource

# Entity: User
user_entity = Entity(
    name="user_id",
    description="User identifier",
    join_keys=["user_id"],
    value_type=ValueType.INT64,
)

# Data source - using Parquet file for Spark offline store
# Spark requires SparkSource instead of FileSource
# SparkSource requires file_format to be specified as a string when using path
# Use absolute path to avoid Spark path resolution issues (Spark resolves from current working directory)
_repo_dir = os.path.dirname(os.path.abspath(__file__))
_data_path = os.path.join(_repo_dir, "data", "user_stats.parquet")

user_stats_source = SparkSource(
    name="user_stats",
    path=_data_path,  # Use absolute path to avoid Spark path resolution issues
    timestamp_field="created_timestamp",
    file_format="parquet",  # SparkSource expects string format, not ParquetFormat() object
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

