import time
import json
import logging
from kafka import KafkaConsumer

# -------------------------------
# Configuration
# -------------------------------
RAW_TOPIC = "weather.processed"
BOOTSTRAP_SERVERS = "kafka:9092"
GROUP_ID = "consumer-group"
AUTO_OFFSET_RESET = "earliest"
ENABLE_AUTO_COMMIT = True
MAX_POLL_RECORDS = 1        # small batches for low latency
FETCH_MAX_WAIT_MS = 50      # flush messages quickly
WAIT_BEFORE_START = 5       # seconds to wait for Kafka
LOG_LEVEL = logging.INFO

# -------------------------------
# Logging setup
# -------------------------------
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logging.getLogger("kafka").setLevel(logging.WARNING)  # reduce Kafka internal logs

# -------------------------------
# Optional wait for Kafka
# -------------------------------
time.sleep(WAIT_BEFORE_START)

# -------------------------------
# Safe JSON deserializer
# -------------------------------
def safe_json_loads(m):
    try:
        return json.loads(m.decode('utf-8'))
    except (json.JSONDecodeError, UnicodeDecodeError, AttributeError, TypeError) as e:
        logging.warning(f"Skipping bad message: {m!r} ({e})")
        return None

# -------------------------------
# Kafka consumer setup
# -------------------------------
consumer = KafkaConsumer(
    RAW_TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    group_id=GROUP_ID,
    auto_offset_reset=AUTO_OFFSET_RESET,
    enable_auto_commit=ENABLE_AUTO_COMMIT,
    value_deserializer=safe_json_loads,
    max_poll_records=MAX_POLL_RECORDS,
    fetch_max_wait_ms=FETCH_MAX_WAIT_MS
)

logging.info("Consumer is ready and listening...")

# -------------------------------
# Consume messages
# -------------------------------
try:
    for message in consumer:
        data = message.value
        if data is None:
            continue  # skip bad message

        logging.info(
            "%s:%d:%d: key=%s value=%s" % (
                message.topic,
                message.partition,
                message.offset,
                message.key,
                data
            )
        )

except KeyboardInterrupt:
    logging.info("Consumer stopped by user.")
finally:
    consumer.close()
