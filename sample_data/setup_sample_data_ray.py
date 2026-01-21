"""
Script to set up sample data in Parquet format for Ray offline store

This script creates MULTIPLE TRANSACTIONS per user at different timestamps.
This enables testing of get_historical_features() with date ranges where
the same entity (user_id) can have multiple data points.
"""
import pandas as pd
from datetime import datetime, timedelta
import random
import os

# Set random seed for reproducibility
random.seed(42)

# Create data directory if it doesn't exist
os.makedirs("feature_repo_ray/data", exist_ok=True)

print("Creating sample user_stats data for Ray...")
print("📊 Generating MULTIPLE transactions per user for time-range testing\n")

# Configuration
NUM_USERS = 50  # Number of unique users
MIN_TRANSACTIONS_PER_USER = 3
MAX_TRANSACTIONS_PER_USER = 10
DATE_RANGE_DAYS = 30  # Spread transactions over this many days

# Generate sample data with multiple transactions per user
base_time = datetime.now()
data = []

for user_id in range(1, NUM_USERS + 1):
    # Each user has multiple transactions at different times
    num_transactions = random.randint(MIN_TRANSACTIONS_PER_USER, MAX_TRANSACTIONS_PER_USER)
    
    for txn_idx in range(num_transactions):
        # Spread transactions across the date range
        days_ago = random.uniform(0, DATE_RANGE_DAYS)
        hours_ago = random.uniform(0, 24)
        created_ts = base_time - timedelta(days=days_ago, hours=hours_ago)
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

# Sort by user_id and timestamp for easier inspection
df = df.sort_values(['user_id', 'created_timestamp']).reset_index(drop=True)

# Save to Parquet
# Ray can handle both nanosecond and microsecond precision, but microsecond is safer
df['created_timestamp'] = df['created_timestamp'].dt.round('us')

parquet_path = "feature_repo_ray/data/user_stats.parquet"
df.to_parquet(parquet_path, index=False, engine='pyarrow', allow_truncated_timestamps=True)

print(f"✅ Created {parquet_path} with {len(df)} records")
print(f"   - {NUM_USERS} unique users")
print(f"   - {MIN_TRANSACTIONS_PER_USER}-{MAX_TRANSACTIONS_PER_USER} transactions per user")

# Show transaction count per user
txn_counts = df.groupby('user_id').size()
print(f"\n📈 Transactions per user stats:")
print(f"   Min: {txn_counts.min()}, Max: {txn_counts.max()}, Avg: {txn_counts.mean():.1f}")

# Show timestamp range
print(f"\n📅 Timestamp range:")
print(f"   From: {df['created_timestamp'].min()}")
print(f"   To:   {df['created_timestamp'].max()}")

# Show sample data for a specific user (user_id=17 as mentioned in the task)
print(f"\n👤 Sample data for user_id=17:")
user_17_data = df[df['user_id'] == 17][['user_id', 'created_timestamp', 'avg_transaction_amount', 'total_transactions']]
print(user_17_data.to_string(index=False))
print(f"   Total transactions for user 17: {len(user_17_data)}")

print("\n✅ Sample data setup complete!")

