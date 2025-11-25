#!/usr/bin/env python3
"""
Quick Postgres Connection Test Script

This script tests the connection to the Postgres database and performs basic operations.

Installation:
pip3 install psycopg2-binary

Usage:
python3 test_postgres_connection.py
"""

import psycopg2
import sys
from datetime import datetime

# Database connection parameters
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'feast',
    'user': 'feast',
    'password': 'feast'
}

def test_connection():
    """Test basic database connection"""
    try:
        print("🔌 Testing Postgres connection...")
        conn = psycopg2.connect(**DB_CONFIG)
        print("✅ Connection successful!")
        
        # Test basic query
        cur = conn.cursor()
        cur.execute("SELECT version();")
        version = cur.fetchone()[0]
        print(f"📊 Database version: {version}")
        
        cur.close()
        conn.close()
        return True
        
    except psycopg2.Error as e:
        print(f"❌ Connection failed: {e}")
        return False
    except ImportError:
        print("❌ psycopg2 not installed. Run: pip3 install psycopg2-binary")
        return False

def test_operations():
    """Test basic database operations"""
    try:
        print("\n🛠️  Testing database operations...")
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # Create test table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS test_connections (
                id SERIAL PRIMARY KEY,
                test_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                message TEXT
            )
        """)
        print("✅ Table created/verified")
        
        # Insert test data
        test_message = f"Connection test at {datetime.now()}"
        cur.execute(
            "INSERT INTO test_connections (message) VALUES (%s) RETURNING id;",
            (test_message,)
        )
        new_id = cur.fetchone()[0]
        print(f"✅ Inserted record with ID: {new_id}")
        
        # Query test data
        cur.execute("SELECT COUNT(*) FROM test_connections;")
        count = cur.fetchone()[0]
        print(f"✅ Total records in test table: {count}")
        
        # Show recent records
        cur.execute("""
            SELECT id, test_time, message 
            FROM test_connections 
            ORDER BY test_time DESC 
            LIMIT 3
        """)
        records = cur.fetchall()
        print("📋 Recent test records:")
        for record in records:
            print(f"   ID: {record[0]}, Time: {record[1]}, Message: {record[2]}")
        
        conn.commit()
        cur.close()
        conn.close()
        return True
        
    except psycopg2.Error as e:
        print(f"❌ Database operations failed: {e}")
        return False

def cleanup_test_data():
    """Clean up test data (optional)"""
    try:
        print("\n🧹 Cleaning up test data...")
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        cur.execute("DROP TABLE IF EXISTS test_connections;")
        print("✅ Test table dropped")
        
        conn.commit()
        cur.close()
        conn.close()
        
    except psycopg2.Error as e:
        print(f"❌ Cleanup failed: {e}")

def main():
    """Main test function"""
    print("🐘 Postgres Connection Test")
    print("=" * 40)
    
    # Check if psycopg2 is available
    try:
        import psycopg2
    except ImportError:
        print("❌ psycopg2 not found!")
        print("💡 Install it with: pip3 install psycopg2-binary")
        sys.exit(1)
    
    # Test connection
    if not test_connection():
        print("\n💡 Troubleshooting:")
        print("   1. Make sure Postgres is running: docker-compose up -d")
        print("   2. Check if port 5432 is available: lsof -i :5432")
        print("   3. Verify docker-compose.yml is in current directory")
        sys.exit(1)
    
    # Test operations
    if not test_operations():
        sys.exit(1)
    
    # Ask if user wants to cleanup
    print("\n" + "=" * 40)
    cleanup = input("🗑️  Clean up test data? (y/N): ").lower().strip()
    if cleanup in ['y', 'yes']:
        cleanup_test_data()
    
    print("\n🎉 All tests completed successfully!")
    print("\n💡 Connection details:")
    for key, value in DB_CONFIG.items():
        if key == 'password':
            print(f"   {key}: {'*' * len(str(value))}")
        else:
            print(f"   {key}: {value}")

if __name__ == "__main__":
    main()