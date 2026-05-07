import psycopg2
from kafka import KafkaProducer
import json
from datetime import datetime

BATCH_SIZE = 5000

def log(msg):
    print(f"[{datetime.now()}] {msg}")

# ================= DB =================
conn = psycopg2.connect(
    dbname="finacle_dw",
    user="postgres",
    password="postgres",
    host="localhost",
    port="5433"
)
cur = conn.cursor()

meta_conn = psycopg2.connect(
    dbname="finacle_dw",
    user="postgres",
    password="postgres",
    host="localhost",
    port="5433"
)
meta_cur = meta_conn.cursor()

# ================= KAFKA =================
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
)

# ================= CHECKPOINT =================
meta_cur.execute("""
    SELECT last_value_date
    FROM test_dtt_metadata
    WHERE table_name = 'test_dtt_transactions';
""")

row = meta_cur.fetchone()

if row:
    last_value_date = row[0]
else:
    last_value_date = datetime(1970, 1, 1).date()

    meta_cur.execute("""
        INSERT INTO test_dtt_metadata (
    pipeline_name,
    last_value_date,
    last_tran_id
)
        VALUES ('dtt', %s);
    """, (last_value_date,))
    meta_conn.commit()

log(f"🚀 Starting from {last_value_date}")

# ================= QUERY (YOUR ORIGINAL LOGIC) =================
query = """
SELECT acid, tran_id, tran_date, value_date, tran_amt,
       part_tran_type, tran_sub_type, tran_particulars
FROM test_dtt_transactions
WHERE value_date > %s
ORDER BY value_date
"""

cur.execute(query, (last_value_date,))

total = 0
max_value_date = last_value_date

while True:
    rows = cur.fetchmany(BATCH_SIZE)
    if not rows:
        break

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

        producer.send('test_dtt_transactions', value=data)

        if row[3] > max_value_date:
            max_value_date = row[3]

    producer.flush()

    meta_cur.execute("""
        UPDATE pipeline_metadata
        SET last_value_date = %s
        WHERE table_name = 'dtt';
    """, (max_value_date,))

    meta_conn.commit()

    last_value_date = max_value_date
    total += len(rows)

    log(f"📤 Sent {total} records | checkpoint = {last_value_date}")

log("✅ Producer completed safely")

cur.close()
meta_cur.close()
conn.close()
meta_conn.close()