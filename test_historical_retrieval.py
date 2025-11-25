"""
Minimal example to test historical feature retrieval with Feast
Used for testing Dask/Spark offline store changes
"""
import pandas as pd
from datetime import timedelta
from feast import FeatureStore
import logging


# ============================================================================
# CONFIGURATION: Choose which feature repository to use
# ============================================================================
# Options:
#   - "./feature_repo"        : Dask offline store
#   - "./feature_repo_spark"  : Spark offline store
#   - "./feature_repo_remote" : Remote Feast server

FEATURE_REPO_PATH = "./feature_repo_spark"  # Change this to switch between Dask/Spark

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
parquet_path = f"./feature_repo_spark/data/user_stats.parquet"
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
    print("   Run setup_sample_data_dask.py first to create sample data")
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
# NOTE: If you see "unexpected keyword argument 'start_date'" error for Spark,
#       this is expected - you're implementing this feature in SparkOfflineStore

print("\n" + "=" * 70)
print("SECTION 4: Test Historical Feature Retrieval Using start_date & end_date")
print("=" * 70)

try:
    # Read Parquet file to determine appropriate date range
    parquet_path = f"./feature_repo_spark/data/user_stats.parquet"
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
        # Join with source data to show created_timestamp vs event_timestamp
        # This helps understand the difference between when features were created vs when they're queried
        df_with_source = None
        if 'user_id' in df.columns:
            # Convert user_id to same type for merging
            df['user_id'] = df['user_id'].astype(int)
            # Merge with source data to show created_timestamp
            df_with_source = df.merge(
                data_df[['user_id', 'created_timestamp']],
                on='user_id',
                how='left',
                suffixes=('', '_source')
            )
            # Rename for clarity
            if 'created_timestamp_source' in df_with_source.columns:
                df_with_source = df_with_source.rename(columns={'created_timestamp_source': 'created_timestamp_in_source'})
            
            print(f"\n📋 First 10 rows (showing both event_timestamp and created_timestamp):")
            # Show key columns - handle both event_timestamp and entity_ts column names
            timestamp_col = 'event_timestamp' if 'event_timestamp' in df_with_source.columns else 'entity_ts'
            display_cols = ['user_id', timestamp_col]
            if 'created_timestamp_in_source' in df_with_source.columns:
                display_cols.append('created_timestamp_in_source')
            display_cols.extend([col for col in df_with_source.columns if col not in display_cols and col != 'created_timestamp'])
            print(df_with_source[display_cols].head(30))
            
            print(f"\n💡 Explanation:")
            print(f"  - event_timestamp: Point-in-time when features are queried (query timestamp)")
            print(f"  - created_timestamp_in_source: When the feature was actually created/updated in source data")
            print(f"  - When using start_date/end_date without entity_df, Feast uses end_date as event_timestamp")
            print(f"  - This is correct: 'What were the features as of {end_date}?'")
        else:
            print(f"\n📋 First 30 rows:")
            print(df.head(30))
        
        print(f"\n📊 Summary:")
        print(f"  Total rows: {len(df)}")
        if 'user_id' in df.columns:
            print(f"  Unique user_ids: {df['user_id'].nunique()}")
        if 'event_timestamp' in df.columns:
            print(f"  Event timestamp range (query time): {df['event_timestamp'].min()} to {df['event_timestamp'].max()}")
            if df_with_source is not None and len(df_with_source) > 0:
                if 'created_timestamp_in_source' in df_with_source.columns:
                    print(f"  Created timestamp range (source data): {df_with_source['created_timestamp_in_source'].min()} to {df_with_source['created_timestamp_in_source'].max()}")
    else:
        print("⚠️  Empty result - no features found in the specified date range")
        print(f"   Data spans: {data_df['created_timestamp'].min()} to {data_df['created_timestamp'].max()}")
        print(f"   Requested range: {start_date} to {end_date}")
        
except FileNotFoundError:
    print(f"⚠️  Parquet file not found at {parquet_path}")
    print("   Run setup_sample_data_dask.py first to create sample data")
except Exception as e:
    print(f"❌ Error in date range-based retrieval: {e}")
    import traceback
    traceback.print_exc()


# ============================================================================
# Summary
# ============================================================================
print("\n" + "=" * 70)
print("✅ Test completed!")
print("=" * 70)
