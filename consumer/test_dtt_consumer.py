from kafka import KafkaConsumer
import psycopg2
import json
from datetime import datetime
from psycopg2.extras import execute_values

# ---------------- CONFIG ----------------
TOPIC = "test_dtt_transactions"   # ✅ fixed topic name
BATCH_SIZE = 5000                  # ✅ keep 1 for testing (change later to 1000+)

def log(msg):
    print(f"[{datetime.now()}] {msg}")

log("🚀 Starting TEST DTT Consumer...")

# ---------------- DB CONNECTION ----------------
conn = psycopg2.connect(
    host="127.0.0.1",
    port="5433",
    database="finacle_dw",
    user="postgres",
    password="postgres"
)
cur = conn.cursor()

# ---------------- KAFKA CONSUMER ----------------
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',     # ✅ read from beginning
    enable_auto_commit=False,         # ✅ manual commit
    group_id='test-dtt-consumer-v2',  # ✅ new group (avoids old offset issues)
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    max_poll_records=BATCH_SIZE
)

buffer = []
count = 0

log("📡 Consuming messages...")

# ---------------- CONSUME LOOP ----------------
for message in consumer:
    try:
        data = message.value

        # ✅ DEBUG LOG (VERY IMPORTANT)
        log(f"📩 Received: {data}")

        buffer.append((
            data.get("acid"),
            data.get("tran_id"),   # ✅ removed int() to avoid crash
            data.get("part_tran_srl_num"),
            data.get("tran_date"),
            data.get("value_date"),
            data.get("tran_amt"),
            data.get("part_tran_type"),
            data.get("tran_sub_type"),
            data.get("tran_particulars")
        ))

        # ---------------- BATCH INSERT ----------------
        if len(buffer) >= BATCH_SIZE:
            execute_values(cur, """
                INSERT INTO test_dtt_transactions (
                    acid, tran_id, part_tran_srl_num, tran_date, value_date, tran_amt,
                    part_tran_type, tran_sub_type, tran_particulars
                ) VALUES %s
                ON CONFLICT (acid,tran_id,part_tran_srl_num,value_date)
                DO NOTHING
            """, buffer)

            conn.commit()

            # ✅ commit Kafka offset ONLY after DB success
            consumer.commit()

            count += len(buffer)
            log(f"📥 Inserted {count} records (offset committed)")

            buffer.clear()

    except Exception as e:
        conn.rollback()
        log(f"❌ Error processing message: {e}")
