from kafka import KafkaConsumer
import psycopg2
import json
from datetime import datetime

def log(msg):
    print(f"[{datetime.now()}] {msg}")

log("🚀 Starting FAST DTT Consumer...")

conn = psycopg2.connect(
    host="127.0.0.1",
    port='5433',
    database="finacle_dw",
    user="postgres",
    password="postgres"
)
cur = conn.cursor()

consumer = KafkaConsumer(
    'dtt-transactions',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    group_id='dtt-consumer-fast'
)

batch_size = 2000
buffer = []
count = 0

log("📡 Consuming messages...")

for message in consumer:
    data = message.value

    buffer.append((
        data.get("acid"),
        data.get("tran_id"),
        data.get("tran_date"),
        data.get("value_date"),
        data.get("tran_amt"),
        data.get("part_tran_type"),
        data.get("tran_sub_type"),
        data.get("tran_particulars"),
        message.offset
    ))

    if len(buffer) >= batch_size:
        cur.executemany("""
            INSERT INTO staging_dtt (
                acid, tran_id, tran_date, value_date, tran_amt,
                part_tran_type, tran_sub_type, tran_particulars,
                kafka_offset
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (tran_id) DO NOTHING
        """, buffer)

        conn.commit()
        count += len(buffer)

        log(f"📥 Inserted {count} records")

        buffer.clear()