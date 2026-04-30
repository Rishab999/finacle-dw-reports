from kafka import KafkaProducer
import psycopg2
import json
from datetime import datetime

def log(msg):
    print(f"[{datetime.now()}] {msg}")

log("Starting DTT Producer (FINACLE)...")

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# ✅ SOURCE DB (Finacle)
log("Connecting to Finacle DB...")
conn = psycopg2.connect(
    host="10.60.133.66",
    port="2951",
    database="finprd",
    user="MIS_Report",
    password="************"
)
cur = conn.cursor()

# 🔴 Metadata still stored in YOUR warehouse (important design)
log("Connecting to metadata (local DW)...")
meta_conn = psycopg2.connect(
    host="localhost",
    database="finacle_dw",
    user="postgres",
    password="postgres"
)
meta_cur = meta_conn.cursor()

# Get last processed value_date
meta_cur.execute("SELECT last_value_date FROM pipeline_metadata WHERE table_name = 'dtt'")
row = meta_cur.fetchone()

last_value_date = row[0] if row else None

if last_value_date:
    log(f"Last processed value_date: {last_value_date}")
else:
    log("No previous run → FULL LOAD")

# Fetch data from Finacle
if last_value_date:
    query = """
        SELECT acid, tran_id, tran_date, value_date, tran_amt,
               part_tran_type, tran_sub_type, tran_particulars
        FROM tbaadm.dtt
        WHERE value_date > %s
        ORDER BY value_date
    """
    cur.execute(query, (last_value_date,))
else:
    query = """
        SELECT acid, tran_id, tran_date, value_date, tran_amt,
               part_tran_type, tran_sub_type, tran_particulars
        FROM tbaadm.dtt
        ORDER BY value_date
    """
    cur.execute(query)

rows = cur.fetchall()
log(f"Fetched {len(rows)} rows from Finacle")

# Send to Kafka
max_value_date = last_value_date
count = 0

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

    # Track latest value_date
    if not max_value_date or r[3] > max_value_date:
        max_value_date = r[3]

    if count % 100 == 0:
        log(f"Sent {count} messages...")

producer.flush()
log(f"Total messages sent: {count}")

# Update metadata in DW
if max_value_date:
    log(f"Updating metadata with: {max_value_date}")
    meta_cur.execute("""
        INSERT INTO pipeline_metadata (table_name, last_value_date)
        VALUES ('dtt', %s)
        ON CONFLICT (table_name)
        DO UPDATE SET last_value_date = EXCLUDED.last_value_date
    """, (max_value_date,))
    meta_conn.commit()

log("Producer completed successfully ✅")