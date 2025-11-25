"""
Script to set up sample data in Parquet format for Spark offline store
Spark requires microsecond precision timestamps (not nanosecond)
"""
import pandas as pd
from datetime import datetime, timedelta
import random
import os

# Create data directory if it doesn't exist
os.makedirs("feature_repo_spark/data", exist_ok=True)

print("Creating sample user_stats data for Spark...")

# Generate sample data
base_time = datetime.now()
data = []
for user_id in range(1, 101):
    # Create timestamps in the past
    created_ts = base_time - timedelta(days=random.randint(0, 30))
    last_active = created_ts - timedelta(hours=random.randint(1, 24))
    
    data.append({
        "user_id": user_id,
        "avg_transaction_amount": round(random.uniform(10.0, 1000.0), 2),
        "total_transactions": random.randint(1, 500),
        "last_active_date": int(last_active.timestamp()),
        "created_timestamp": created_ts,
    })

# Create DataFrame
df = pd.DataFrame(data)

# Convert timestamp to microsecond precision for Spark compatibility
# Spark doesn't support nanosecond precision timestamps
df['created_timestamp'] = pd.to_datetime(df['created_timestamp']).dt.floor('us')

# Save to Parquet with Spark-compatible settings
parquet_path = "feature_repo_spark/data/user_stats.parquet"
df.to_parquet(
    parquet_path, 
    index=False,
    engine='pyarrow',  # Use PyArrow for better Spark compatibility
    coerce_timestamps='us',  # Coerce timestamps to microsecond precision
    allow_truncated_timestamps=True  # Allow truncation if needed
)

print(f"✅ Created {parquet_path} with {len(df)} records")
print(f"📅 Timestamp precision: microseconds (Spark-compatible)")

# Show some sample data
print("\nSample data:")
print(df.head())

print("\n✅ Sample data setup complete for Spark!")

