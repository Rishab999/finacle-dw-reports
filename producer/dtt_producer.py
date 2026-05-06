from kafka import KafkaProducer
import psycopg2
import json
from datetime import datetime

def log(msg):
    print(f"[{datetime.now()}] {msg}")

log("🚀 Starting DTT Producer (FINACLE)...")

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    linger_ms=10,
    batch_size=16384
)

# SOURCE DB (Finacle)
conn = psycopg2.connect(
    host="10.60.133.66",
    port="2951",
    database="finprd",
    user="MIS_Report",
    password="MIS_reaDonly@123"
)

cur = conn.cursor(name='dtt_cursor')  # ✅ server-side cursor
cur.itersize = 5000

# METADATA DB (Postgres - Docker)
meta_conn = psycopg2.connect(
    host="localhost",
    port='5433',
    database="finacle_dw",
    user="postgres",
    password="postgres"
)
meta_cur = meta_conn.cursor()

# Get last processed value_date
meta_cur.execute("""
    SELECT last_value_date 
    FROM pipeline_metadata 
    WHERE table_name = 'dtt'
""")

row = meta_cur.fetchone()
last_value_date = row[0] if row else None

if last_value_date:
    log(f"Last processed value_date: {last_value_date}")
else:
    log("No previous run → FULL LOAD")

# Execute query
if last_value_date:
    cur.execute("""
        SELECT acid, tran_id, tran_date, value_date, tran_amt,
               part_tran_type, tran_sub_type, tran_particulars
        FROM tbaadm.dtt
        WHERE value_date > %s
        ORDER BY value_date
    """, (last_value_date,))
else:
    cur.execute("""
        SELECT acid, tran_id, tran_date, value_date, tran_amt,
               part_tran_type, tran_sub_type, tran_particulars
        FROM tbaadm.dtt
        ORDER BY value_date
    """)

BATCH_SIZE = 5000
count = 0
max_value_date = last_value_date

# Stream data in batches
while True:
    rows = cur.fetchmany(BATCH_SIZE)

    if not rows:
        break

    for r in rows:
        data = {
            "acid": r[0],
            "tran_id": r[1],
            "tran_date": str(r[2]),
            "value_date": str(r[3]),
            "tran_amt": float(r[4]) if r[4] else None,
            "part_tran_type": r[5],
            "tran_sub_type": r[6],
            "tran_particulars": r[7]
        }

        producer.send('dtt-transactions', value=data)
        count += 1

        if not max_value_date or r[3] > max_value_date:
            max_value_date = r[3]

    producer.flush()
    log(f"📤 Sent {count} records...")

log(f"✅ Total messages sent: {count}")

# Update metadata
if max_value_date:
    meta_cur.execute("""
        INSERT INTO pipeline_metadata (table_name, last_value_date)
        VALUES ('dtt', %s)
        ON CONFLICT (table_name)
        DO UPDATE SET last_value_date = EXCLUDED.last_value_date
    """, (max_value_date,))
    meta_conn.commit()

log("✅ Producer completed successfully")