import psycopg2
from kafka import KafkaProducer
import json
from datetime import datetime

BATCH_SIZE = 5000

def log(msg):
    print(f"[{datetime.now()}] {msg}")

log("🚀 Starting DTT Producer (FINACLE)...")

# ================= KAFKA =================
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
)

# ================= SOURCE DB =================
conn = psycopg2.connect(
    host="10.60.133.66",
    port="2951",
    database="finprd",
    user="MIS_Report",
    password="MIS_reaDonly@123"
)

cur = conn.cursor(name='dtt_cursor')
cur.itersize = BATCH_SIZE

# ================= METADATA DB =================
meta_conn = psycopg2.connect(
    host="localhost",
    port="5433",
    database="finacle_dw",
    user="postgres",
    password="postgres"
)

meta_cur = meta_conn.cursor()

# ================= GET CHECKPOINT =================
meta_cur.execute("""
SELECT
    last_value_date,
    last_tran_id,
    last_part_tran_srl_num
FROM test_dtt_metadata
WHERE pipeline_name = 'dtt'
""")

row = meta_cur.fetchone()

if row:

    last_value_date, last_tran_id, last_part_tran_srl_num = row

    last_tran_id = str(last_tran_id or '0')
    last_part_tran_srl_num = str(last_part_tran_srl_num or '0')

else:

    last_value_date = datetime(1970, 1, 1).date()
    last_tran_id = '0'
    last_part_tran_srl_num = '0'

    meta_cur.execute("""
        INSERT INTO test_dtt_metadata(
            pipeline_name,
            last_value_date,
            last_tran_id,
            last_part_tran_srl_num
        )
        VALUES (%s,%s,%s,%s)
    """, (
        'dtt',
        last_value_date,
        last_tran_id,
        last_part_tran_srl_num
    ))

    meta_conn.commit()

log(
    f"🚀 Starting from "
    f"({last_value_date}, {last_tran_id}, {last_part_tran_srl_num})"
)

# ================= QUERY =================
query = """
SELECT
    acid,
    tran_id,
    part_tran_srl_num,
    tran_date,
    value_date,
    tran_amt,
    part_tran_type,
    tran_sub_type,
    tran_particulars
FROM tbaadm.dtt
WHERE value_date >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY
    value_date,
    tran_id,
    part_tran_srl_num
"""

cur.execute(query)

total = 0

max_value_date = last_value_date
max_tran_id = last_tran_id
max_part_tran_srl_num = last_part_tran_srl_num

# ================= STREAM =================
while True:

    rows = cur.fetchmany(BATCH_SIZE)

    if not rows:
        break

    for r in rows:

        value_date = r[4]

        if isinstance(value_date, datetime):
            value_date = value_date.date()

        data = {
            "acid": r[0],
            "tran_id": str(r[1]),
            "part_tran_srl_num": str(r[2]),
            "tran_date": str(r[3]),
            "value_date": str(value_date),
            "tran_amt": float(r[5]) if r[5] else None,
            "part_tran_type": r[6],
            "tran_sub_type": r[7],
            "tran_particulars": r[8]
        }

        producer.send(
            'test_dtt_transactions',
            value=data
        )

        total += 1

        max_value_date = value_date
        max_tran_id = str(r[1])
        max_part_tran_srl_num = str(r[2])

    producer.flush()

    meta_cur.execute("""
        UPDATE test_dtt_metadata
        SET
            last_value_date = %s,
            last_tran_id = %s,
            last_part_tran_srl_num = %s
        WHERE pipeline_name = 'dtt'
    """, (
        max_value_date,
        max_tran_id,
        max_part_tran_srl_num
    ))

    meta_conn.commit()

    log(
        f"📤 Sent {total} records | checkpoint = "
        f"({max_value_date}, {max_tran_id}, {max_part_tran_srl_num})"
    )

log(f"✅ Producer completed successfully | Total sent = {total}")

cur.close()
meta_cur.close()

conn.close()
meta_conn.close()

producer.close()