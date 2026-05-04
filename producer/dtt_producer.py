import psycopg2
from kafka import KafkaProducer
import json
from datetime import datetime

BATCH_SIZE = 5000

def log(msg):
    print(f"[{datetime.now()}] {msg}")

# DB connection (FINACLE)
conn = psycopg2.connect(...)
cur = conn.cursor()

# Metadata DB (same DB or separate)
meta_conn = psycopg2.connect(...)
meta_cur = meta_conn.cursor()

# Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Get last checkpoint
meta_cur.execute("""
SELECT last_value_date FROM pipeline_metadata WHERE table_name='dtt'
""")
last_value_date = meta_cur.fetchone()[0]

log(f"🚀 Starting from {last_value_date}")

query = """
SELECT acid, tran_id, tran_date, value_date, tran_amt,
       part_tran_type, tran_sub_type, tran_particulars
FROM dtt_table
WHERE value_date > %s
ORDER BY value_date
"""

cur.execute(query, (last_value_date,))

total = 0

while True:
    rows = cur.fetchmany(BATCH_SIZE)
    if not rows:
        break

    max_value_date = last_value_date

    for row in rows:
        data = {
            "acid": row[0],
            "tran_id": row[1],
            "tran_date": str(row[2]),
            "value_date": str(row[3]),
            "tran_amt": float(row[4]) if row[4] else None,
            "part_tran_type": row[5],
            "tran_sub_type": row[6],
            "tran_particulars": row[7]
        }

        producer.send('dtt-transactions', value=data)

        # Track latest value_date in batch
        if row[3] and row[3] > max_value_date:
            max_value_date = row[3]

    producer.flush()

    # ✅ CHECKPOINT UPDATE (CRITICAL)
    meta_cur.execute("""
        UPDATE pipeline_metadata
        SET last_value_date = %s
        WHERE table_name = 'dtt'
    """, (max_value_date,))
    meta_conn.commit()

    last_value_date = max_value_date
    total += len(rows)

    log(f"📤 Sent {total} records | checkpoint = {last_value_date}")

log("✅ Producer completed safely")