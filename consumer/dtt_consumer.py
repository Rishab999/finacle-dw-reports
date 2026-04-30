from kafka import KafkaConsumer
import psycopg2
import json
from datetime import datetime

def log(msg):
    print(f"[{datetime.now()}] {msg}")

log("🚀 Starting DTT Consumer...")

# ✅ TARGET DB (Data Warehouse)
try:
    conn = psycopg2.connect(
        host="localhost",
        database="finacle_dw",
        user="postgres",
        password="postgres"
    )
    cur = conn.cursor()
    log("✅ Connected to Data Warehouse (finacle_dw)")
except Exception as e:
    log(f"❌ DB Connection Failed: {e}")
    exit()

# ✅ Kafka Consumer
try:
    consumer = KafkaConsumer(
        'dtt-transactions',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        group_id='dtt-consumer-group'
    )
    log("✅ Connected to Kafka topic: dtt-transactions")
except Exception as e:
    log(f"❌ Kafka Connection Failed: {e}")
    exit()

log("📡 Waiting for messages...")

count = 0

for message in consumer:
    data = message.value

    try:
        cur.execute("""
            INSERT INTO staging_dtt (
                acid, tran_id, tran_date, value_date, tran_amt,
                part_tran_type, tran_sub_type, tran_particulars,
                kafka_offset
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
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

        conn.commit()
        count += 1

        # 🔹 Log progress
        if count % 50 == 0:
            log(f"📥 Inserted {count} records into staging_dtt")

    except Exception as e:
        log(f"❌ Insert Error at offset {message.offset}: {e}")
        conn.rollback()
