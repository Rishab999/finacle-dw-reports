from kafka import KafkaConsumer
import psycopg2
import json
from datetime import datetime

def log(msg):
    print(f"[{datetime.now()}] {msg}")

log("🚀 Starting DTT Consumer...")

# Connect to PostgreSQL (DW)
conn = psycopg2.connect(
    host="127.0.0.1",
    port='5433',
    database="finacle_dw",
    user="postgres",
    password="postgres"
)
cur = conn.cursor()

# Kafka Consumer
consumer = KafkaConsumer(
    'dtt-transactions',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='dtt-consumer-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    max_poll_records=500
)

log("📡 Waiting for messages...")

count = 0
batch_size = 100

insert_query = """
INSERT INTO staging_dtt (
    acid, tran_id, tran_date, value_date, tran_amt,
    part_tran_type, tran_sub_type, tran_particulars,
    kafka_offset
) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT (tran_id) DO NOTHING
"""

for message in consumer:
    data = message.value

    try:
        cur.execute(insert_query, (
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

        count += 1

        # Batch commit
        if count % batch_size == 0:
            conn.commit()
            log(f"📥 Inserted {count} records")

    except Exception as e:
        log(f"❌ Error at offset {message.offset}: {e}")
        conn.rollback()

# Final commit (safety)
conn.commit()