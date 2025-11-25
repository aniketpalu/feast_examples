"""
Script to set up sample data in Parquet format for Dask offline store
"""
import pandas as pd
from datetime import datetime, timedelta
import random
import os

# Create data directory if it doesn't exist
os.makedirs("feature_repo/data", exist_ok=True)

print("Creating sample user_stats data...")

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

# Save to Parquet
parquet_path = "feature_repo/data/user_stats.parquet"
df.to_parquet(parquet_path, index=False)

print(f"✅ Created {parquet_path} with {len(df)} records")

# Show some sample data
print("\nSample data:")
print(df.head())

print("\n✅ Sample data setup complete!")

