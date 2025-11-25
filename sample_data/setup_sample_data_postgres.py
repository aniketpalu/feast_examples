"""
Script to set up sample data in PostgreSQL for testing historical feature retrieval
"""
import psycopg2
from datetime import datetime, timedelta
import random

# Connection parameters
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="feast",
    user="feast",
    password="feast"
)

cur = conn.cursor()

print("Creating user_stats table...")

# Create table
cur.execute("""
DROP TABLE IF EXISTS user_stats;
""")

cur.execute("""
CREATE TABLE user_stats (
    user_id INTEGER,
    avg_transaction_amount REAL,
    total_transactions INTEGER,
    last_active_date TIMESTAMP,
    created_timestamp TIMESTAMP
);
""")

print("Inserting sample data...")

# Insert sample data
base_time = datetime.now()
for user_id in range(1, 101):
    # Create timestamps in the past
    created_ts = base_time - timedelta(days=random.randint(0, 30))
    last_active = created_ts - timedelta(hours=random.randint(1, 24))
    
    cur.execute("""
        INSERT INTO user_stats 
        (user_id, avg_transaction_amount, total_transactions, last_active_date, created_timestamp)
        VALUES (%s, %s, %s, %s, %s)
    """, (
        user_id,
        round(random.uniform(10.0, 1000.0), 2),
        random.randint(1, 500),
        last_active,
        created_ts
    ))

conn.commit()
print(f"✅ Inserted 100 sample records")

# Show some sample data
cur.execute("SELECT * FROM user_stats ORDER BY user_id LIMIT 5")
rows = cur.fetchall()
print("\nSample data:")
for row in rows:
    print(row)

cur.close()
conn.close()
print("\n✅ Sample data setup complete!")

