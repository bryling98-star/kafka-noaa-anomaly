import time
import json
import random
import logging
from kafka import KafkaProducer

# -------------------------------
# Configuration
# -------------------------------
BOOTSTRAP_SERVERS = "kafka:9092"
TOPIC = "weather.raw"
SEND_INTERVAL = 2  # seconds
LOG_LEVEL = logging.INFO
RETRIES = 2
LINGER_MS = 0
BATCH_SIZE = 0

# -------------------------------
# Logging setup
# -------------------------------
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# -------------------------------
# Kafka producer setup
# -------------------------------
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    retries=RETRIES,
    linger_ms=LINGER_MS,
    batch_size=BATCH_SIZE
)

# -------------------------------
# Callbacks
# -------------------------------
def on_send_success(record_metadata):
    logging.info(
        f"[Producer] Sent: topic={record_metadata.topic}, "
        f"partition={record_metadata.partition}, offset={record_metadata.offset}"
    )

def on_send_error(excp):
    logging.error(f"[Producer] Error sending message: {excp}", exc_info=True)

# -------------------------------
# Message generation loop
# -------------------------------
logging.info("[Producer] Starting...")

while True:
    try:
        data = {
            "temperature": round(random.uniform(-20, 40), 1),
            "humidity": round(random.uniform(10, 90), 1),
            "cloudsize": 1
        }
        producer.send(TOPIC, value=data).add_callback(on_send_success).add_errback(on_send_error)
        producer.flush()
        logging.info(f"[Producer] Message content: {data}")
    except Exception as e:
        logging.error(f"[Producer] Unexpected error: {e}", exc_info=True)
    time.sleep(SEND_INTERVAL)
