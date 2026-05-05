from kafka import KafkaConsumer
import psycopg2
import json
from datetime import datetime
from psycopg2.extras import execute_values

TOPIC = "test_dtt_transactions"
BATCH_SIZE = 2000

def log(msg):
    print(f"[{datetime.now()}] {msg}")

log("🚀 Starting TEST DTT Consumer...")

# ---------------- DB ----------------
conn = psycopg2.connect(
    host="127.0.0.1",
    port="5433",
    database="finacle_dw",
    user="postgres",
    password="postgres"
)
cur = conn.cursor()

# ---------------- KAFKA ----------------
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=False,   # ❗ manual control
    group_id='test-dtt-consumer',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    max_poll_records=BATCH_SIZE
)

buffer = []
count = 0

log("📡 Consuming messages...")

for message in consumer:
    data = message.value

    buffer.append((
        data.get("acid"),
        int(data.get("tran_id")) if data.get("tran_id") else None,
        data.get("tran_date"),
        data.get("value_date"),
        data.get("tran_amt"),
        data.get("part_tran_type"),
        data.get("tran_sub_type"),
        data.get("tran_particulars")
    ))

    if len(buffer) >= BATCH_SIZE:
        try:
            execute_values(cur, """
                INSERT INTO test_dtt_transactions (
                    acid, tran_id, tran_date, value_date, tran_amt,
                    part_tran_type, tran_sub_type, tran_particulars
                ) VALUES %s
                ON CONFLICT (tran_id) DO NOTHING
            """, buffer)

            conn.commit()

            # ✅ commit Kafka offset ONLY after DB success
            consumer.commit()

            count += len(buffer)
            log(f"📥 Inserted {count} records (offset committed)")

            buffer.clear()

        except Exception as e:
            conn.rollback()
            log(f"❌ Error: {e}")