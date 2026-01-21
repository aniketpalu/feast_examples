"""
Minimal example to test historical feature retrieval with Feast
Used for testing Dask/Spark offline store changes

Supports testing:
1. Basic feature retrieval with entity_df
2. Date range-based retrieval (start_date/end_date)
3. Multiple transactions per entity within a time range
"""
import pandas as pd
from datetime import datetime, timedelta
from feast import FeatureStore
import logging


# ============================================================================
# CONFIGURATION: Choose which feature repository to use
# ============================================================================
# Options:
#   - "./feature_repo_dask"   : Dask offline store
#   - "./feature_repo_spark"  : Spark offline store
#   - "./feature_repo_ray"    : Ray offline store
#   - "./feature_repo_remote" : Remote Feast server

FEATURE_REPO_PATH = "./feature_repo_spark"  # Change this to switch between Dask/Spark/Ray

# Data path - should match the offline store being used
DATA_PATH = f"{FEATURE_REPO_PATH}/data/user_stats.parquet"

# ============================================================================
# SECTION 1: Feast Object Creation
# ============================================================================
# Uncomment/comment this section as needed during development

# Set up logging
logging.getLogger("py4j").setLevel(logging.ERROR)
logging.getLogger("feast").setLevel(logging.INFO)
logger = logging.getLogger("feast")

# Initialize feature store
fs = FeatureStore(repo_path=FEATURE_REPO_PATH)

print("=" * 70)
print("SECTION 1: Feast Object Creation")
print("=" * 70)
print("✅ Feature store initialized")
print(f"🔍 Using offline store type: {fs.config.offline_store.type}")
print(f"📁 Registry: {fs.config.registry}")
print(f"💾 Online store: {fs.config.online_store.type if hasattr(fs.config.online_store, 'type') else 'N/A'}")


# ============================================================================
# SECTION 2: Test Feast Connection & Feature Views
# ============================================================================
# Uncomment/comment this section to test if Feast is connected correctly

print("\n" + "=" * 70)
print("SECTION 2: Test Feast Connection & Feature Views")
print("=" * 70)

try:
    fvs = fs.list_feature_views()
    print(f"✅ Feature views found: {len(fvs)}")
    for fv in fvs:
        print(f"  - {fv.name}")
        # Handle entities - they might be Entity objects or strings
        entity_names = []
        for e in fv.entities:
            if hasattr(e, 'name'):
                entity_names.append(e.name)
            else:
                entity_names.append(str(e))
        print(f"    Entities: {entity_names}")
        print(f"    Source: {type(fv.batch_source).__name__}")
except Exception as e:
    print(f"❌ Error listing feature views: {e}")
    import traceback
    traceback.print_exc()


# ============================================================================
# SECTION 3: Test Historical Feature Retrieval Using Entity DataFrame
# ============================================================================
# Uncomment/comment this section to test entity-based historical retrieval

print("\n" + "=" * 70)
print("SECTION 3: Test Historical Feature Retrieval Using Entity DataFrame")
print("=" * 70)

# Read Parquet file to get sample data
parquet_path = DATA_PATH
try:
    data_df = pd.read_parquet(parquet_path)
    print(f"📁 Data in Parquet file: {len(data_df)} records")
    print(f"📅 Timestamp range: {data_df['created_timestamp'].min()} to {data_df['created_timestamp'].max()}")
    print(f"👥 User IDs range: {data_df['user_id'].min()} to {data_df['user_id'].max()}")
    
    # Create entity dataframe for point-in-time join
    # Use first 3 user_ids with timestamps after their created_timestamp
    sample_data = data_df[['user_id', 'created_timestamp']].head(3)
    entity_df = pd.DataFrame({
        "user_id": sample_data['user_id'].tolist(),
        "event_timestamp": [
            ts + timedelta(hours=1)  # 1 hour after each user's created_timestamp
            for ts in sample_data['created_timestamp']
        ]
    })
    
    print(f"\n📊 Entity data for feature retrieval:")
    print(entity_df)
    
    # Retrieve historical features using entity_df
    print(f"\n🔍 Retrieving features for {len(entity_df)} entities...")
    historical_features = fs.get_historical_features(
        entity_df=entity_df,
        features=[
            "user_stats:avg_transaction_amount",
            "user_stats:total_transactions",
        ],
    )
    
    print(f"\n✅ Historical features retrieved:")
    df = historical_features.to_df()
    print(f"📊 Result shape: {df.shape}")
    if len(df) > 0:
        print(f"\n📋 Results:")
        print(df)
    else:
        print("⚠️  Empty result - no features found")
        
except FileNotFoundError:
    print(f"⚠️  Parquet file not found at {parquet_path}")
    print("   Run the appropriate setup_sample_data script first to create sample data")
except Exception as e:
    print(f"❌ Error in entity-based retrieval: {e}")
    import traceback
    traceback.print_exc()


# ============================================================================
# SECTION 4: Test Historical Feature Retrieval Using start_date & end_date
# ============================================================================
# Uncomment/comment this section to test date range-based historical retrieval
# This is the main test for Dask/Spark offline store changes
# 
# NOTE: If you see "unexpected keyword argument 'start_date'" error
#       this is expected - you're implementing this feature

print("\n" + "=" * 70)
print("SECTION 4: Test Historical Feature Retrieval Using start_date & end_date")
print("=" * 70)

try:
    # Read Parquet file to determine appropriate date range
    parquet_path = DATA_PATH
    data_df = pd.read_parquet(parquet_path)
    
    # Set date range based on actual data
    start_date = data_df['created_timestamp'].min() + timedelta(days=10)
    end_date = data_df['created_timestamp'].max() - timedelta(days=0)
    
    print(f"📁 Data in Parquet file: {len(data_df)} records")
    print(f"📅 Data timestamp range: {data_df['created_timestamp'].min()} to {data_df['created_timestamp'].max()}")
    print(f"📅 Query date range: {start_date} to {end_date}")
    
    # Retrieve historical features using start_date and end_date (no entity_df)
    print(f"\n🔍 Retrieving features for date range (no entity_df)...")
    historical_features = fs.get_historical_features(
        features=[
            "user_stats:avg_transaction_amount",
            "user_stats:total_transactions",
        ],
        start_date=start_date,
        end_date=end_date,
    )
    
    print(f"\n✅ Historical features retrieved:")
    df = historical_features.to_df()
    print(f"📊 Result shape: {df.shape}")
    
    if len(df) > 0:
        if 'user_id' in df.columns:
            df['user_id'] = df['user_id'].astype(int)
        
        # Show the ACTUAL retrieved data (no merge to avoid Cartesian product confusion)
        print(f"\n📋 First 30 rows of ACTUAL retrieved data:")
        print(df.head(30).to_string(index=False))   
        
        print(f"\n📊 Summary:")
        print(f"  Total rows retrieved: {len(df)}")
        if 'user_id' in df.columns:
            print(f"  Unique user_ids: {df['user_id'].nunique()}")
            
            # Show transactions per user distribution
            txn_per_user = df.groupby('user_id').size()
            multi_txn_users = txn_per_user[txn_per_user > 1]
            print(f"  Users with multiple transactions: {len(multi_txn_users)}")
            
        if 'event_timestamp' in df.columns:
            print(f"  Event timestamp range: {df['event_timestamp'].min()} to {df['event_timestamp'].max()}")
        if 'created_timestamp' in df.columns:
            print(f"  Created timestamp range: {df['created_timestamp'].min()} to {df['created_timestamp'].max()}")
        
        # Show sample of users with multiple transactions
        if 'user_id' in df.columns:
            txn_per_user = df.groupby('user_id').size()
            multi_txn_users = txn_per_user[txn_per_user > 1]
            if len(multi_txn_users) > 0:
                sample_user = multi_txn_users.index[0]
                print(f"\n📋 Example: All transactions for user_id={sample_user}:")
                print(df[df['user_id'] == sample_user].to_string(index=False))
    else:
        print("⚠️  Empty result - no features found in the specified date range")
        print(f"   Data spans: {data_df['created_timestamp'].min()} to {data_df['created_timestamp'].max()}")
        print(f"   Requested range: {start_date} to {end_date}")
        
except FileNotFoundError:
    print(f"⚠️  Parquet file not found at {parquet_path}")
    print("   Run the appropriate setup_sample_data script first to create sample data")
except Exception as e:
    print(f"❌ Error in date range-based retrieval: {e}")
    import traceback
    traceback.print_exc()


# ============================================================================
# SECTION 5: Test Multiple Transactions Per Entity Within Time Range
# ============================================================================
# This test validates that when an entity (user_id) has multiple transactions
# within a time range, ALL of those data points are retrieved.

print("\n" + "=" * 70)
print("SECTION 5: Test Multiple Transactions Per Entity Within Time Range")
print("=" * 70)

# Test with a specific user_id (e.g., user_id=17) that has multiple transactions
TARGET_USER_ID = 17

try:
    # Read Parquet file to find all transactions for the target user
    parquet_path = DATA_PATH
    data_df = pd.read_parquet(parquet_path)
    
    # Filter data for target user
    user_data = data_df[data_df['user_id'] == TARGET_USER_ID].copy()
    user_data = user_data.sort_values('created_timestamp')
    
    print(f"\n👤 Source data for user_id={TARGET_USER_ID}:")
    print(f"   Total transactions in source: {len(user_data)}")
    
    if len(user_data) == 0:
        print(f"   ⚠️  No data found for user_id={TARGET_USER_ID}")
    else:
        print(f"\n📋 All transactions for user_id={TARGET_USER_ID} in source data:")
        print(user_data[['user_id', 'created_timestamp', 'avg_transaction_amount', 'total_transactions']].to_string(index=False))
        
        # Define a time range that should capture MULTIPLE transactions for this user
        # We'll use a range that spans from the first to the last transaction
        user_min_ts = user_data['created_timestamp'].min()
        user_max_ts = user_data['created_timestamp'].max()
        
        # Add a small buffer to ensure we capture edge cases
        start_date = user_min_ts - timedelta(hours=1)
        end_date = user_max_ts + timedelta(hours=1)
        
        print(f"\n📅 Time range for query:")
        print(f"   Start: {start_date}")
        print(f"   End:   {end_date}")
        print(f"   Expected transactions in range: {len(user_data)}")
        
        # Create entity_df with multiple timestamps for the same user
        # Each timestamp corresponds to a point-in-time when we want to query features
        # We query at each transaction time + 1 minute to get the feature value AT that time
        entity_df = pd.DataFrame({
            "user_id": [TARGET_USER_ID] * len(user_data),
            "event_timestamp": [ts + timedelta(minutes=1) for ts in user_data['created_timestamp']]
        })
        
        print(f"\n📊 Entity DataFrame for retrieval (querying at each transaction time + 1 min):")
        print(entity_df.to_string(index=False))
        
        # Retrieve historical features
        print(f"\n🔍 Retrieving features for user_id={TARGET_USER_ID} at {len(entity_df)} points in time...")
        historical_features = fs.get_historical_features(
            entity_df=entity_df,
            features=[
                "user_stats:avg_transaction_amount",
                "user_stats:total_transactions",
            ],
        )
        
        result_df = historical_features.to_df()
        
        print(f"\n✅ Historical features retrieved:")
        print(f"📊 Result shape: {result_df.shape}")
        print(f"   Rows retrieved: {len(result_df)}")
        print(f"   Expected rows: {len(user_data)}")
        
        if len(result_df) > 0:
            print(f"\n📋 Retrieved feature values:")
            print(result_df.to_string(index=False))
            
            # Validate results
            if len(result_df) == len(user_data):
                print(f"\n✅ SUCCESS: Retrieved {len(result_df)} rows for {len(user_data)} transactions!")
            else:
                print(f"\n⚠️  MISMATCH: Expected {len(user_data)} rows but got {len(result_df)}")
                
            # Show side-by-side comparison with source data
            # Sort both by timestamp for proper alignment
            print(f"\n📊 Side-by-side comparison (sorted by timestamp):")
            
            # Prepare source data (sorted)
            source_sorted = user_data[['user_id', 'created_timestamp', 'avg_transaction_amount', 'total_transactions']].copy()
            source_sorted = source_sorted.sort_values('created_timestamp').reset_index(drop=True)
            source_sorted.columns = ['user_id', 'source_timestamp', 'source_amount', 'source_txn_count']
            
            # Prepare retrieved data (sorted by event_timestamp)
            retrieved_sorted = result_df[['user_id', 'event_timestamp', 'avg_transaction_amount', 'total_transactions']].copy()
            retrieved_sorted = retrieved_sorted.sort_values('event_timestamp').reset_index(drop=True)
            retrieved_sorted.columns = ['user_id', 'retrieved_timestamp', 'retrieved_amount', 'retrieved_txn_count']
            
            # Combine side-by-side (same index after sorting)
            comparison = pd.concat([
                source_sorted[['source_timestamp', 'source_amount']],
                retrieved_sorted[['retrieved_timestamp', 'retrieved_amount']]
            ], axis=1)
            comparison['amount_match'] = comparison['source_amount'] == comparison['retrieved_amount']
            
            print(comparison.to_string(index=True))
            
            # Summary
            matches = comparison['amount_match'].sum()
            print(f"\n✅ Matches: {matches}/{len(comparison)} rows have matching feature values")
        else:
            print("⚠️  Empty result - no features found")
            
except FileNotFoundError:
    print(f"⚠️  Parquet file not found at {parquet_path}")
    print("   Run: python sample_data/setup_sample_data_ray.py")
except Exception as e:
    print(f"❌ Error in multi-transaction retrieval: {e}")
    import traceback
    traceback.print_exc()


# ============================================================================
# SECTION 6: Test Date Range Query Without Entity DataFrame
# ============================================================================
# This tests using start_date/end_date to get ALL transactions for ALL entities
# within a time range (no entity_df provided)

print("\n" + "=" * 70)
print("SECTION 6: Test Date Range Query - All Entities in Time Window")
print("=" * 70)

try:
    parquet_path = DATA_PATH
    data_df = pd.read_parquet(parquet_path)
    
    # Pick a narrow time window (e.g., 3 days in the middle of the data)
    data_min = data_df['created_timestamp'].min()
    data_max = data_df['created_timestamp'].max()
    mid_point = data_min + (data_max - data_min) / 2
    
    start_date = mid_point - timedelta(days=1.5)
    end_date = mid_point + timedelta(days=1.5)
    
    # Count how many records fall in this range
    records_in_range = data_df[
        (data_df['created_timestamp'] >= start_date) & 
        (data_df['created_timestamp'] <= end_date)
    ]
    
    print(f"\n📅 Query time window (3-day window):")
    print(f"   Start: {start_date}")
    print(f"   End:   {end_date}")
    print(f"   Records in source within this range: {len(records_in_range)}")
    print(f"   Unique users in range: {records_in_range['user_id'].nunique()}")
    
    # Show breakdown by user for records in range
    if len(records_in_range) > 0:
        print(f"\n📊 Transactions per user within time window:")
        user_counts = records_in_range.groupby('user_id').size().reset_index(name='count')
        multi_txn_users = user_counts[user_counts['count'] > 1]
        print(f"   Users with multiple transactions: {len(multi_txn_users)}")
        if len(multi_txn_users) > 0:
            print(f"   Sample users with multiple transactions:")
            print(multi_txn_users.head(5).to_string(index=False))
    
    # Retrieve features using start_date/end_date
    print(f"\n🔍 Retrieving features for date range (no entity_df)...")
    historical_features = fs.get_historical_features(
        features=[
            "user_stats:avg_transaction_amount",
            "user_stats:total_transactions",
        ],
        start_date=start_date,
        end_date=end_date,
    )
    
    result_df = historical_features.to_df()
    
    print(f"\n✅ Historical features retrieved:")
    print(f"📊 Result shape: {result_df.shape}")
    
    if len(result_df) > 0:
        print(f"   Total rows: {len(result_df)}")
        if 'user_id' in result_df.columns:
            print(f"   Unique users: {result_df['user_id'].nunique()}")
            
            # Check for users with multiple rows (multiple transactions)
            user_row_counts = result_df.groupby('user_id').size().reset_index(name='count')
            multi_row_users = user_row_counts[user_row_counts['count'] > 1]
            
            if len(multi_row_users) > 0:
                print(f"\n✅ Users with MULTIPLE rows in result: {len(multi_row_users)}")
                print(f"   Sample:")
                for _, row in multi_row_users.head(3).iterrows():
                    user_id = row['user_id']
                    count = row['count']
                    user_results = result_df[result_df['user_id'] == user_id]
                    print(f"\n   user_id={user_id} has {count} rows:")
                    print(user_results.to_string(index=False))
            else:
                print(f"\n⚠️  No users with multiple rows in result")
                print(f"   (This may be expected if the feature view/query deduplicates)")
    else:
        print("⚠️  Empty result - no features found in the specified date range")

except FileNotFoundError:
    print(f"⚠️  Parquet file not found at {parquet_path}")
except Exception as e:
    print(f"❌ Error in date range retrieval: {e}")
    import traceback
    traceback.print_exc()


# ============================================================================
# Summary
# ============================================================================
print("\n" + "=" * 70)
print("✅ Test completed!")
print("=" * 70)
