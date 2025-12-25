import logging
import json
import time
from kafka import KafkaConsumer, KafkaProducer

# -------------------------------
# Configuration
# -------------------------------
RAW_TOPIC = "weather.raw"
PROCESSED_TOPIC = "weather.processed"
BOOTSTRAP_SERVERS = "kafka:9092"
GROUP_ID = "processor-group"
BATCH_SIZE = 10

# -------------------------------
# Logging setup
# -------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logging.getLogger("kafka").setLevel(logging.WARNING)  # suppress verbose Kafka INFO logs

# -------------------------------
# Safe JSON deserializer
# -------------------------------
def safe_json_loads(m):
    try:
        return json.loads(m.decode("utf-8"))
    except (json.JSONDecodeError, AttributeError, TypeError) as e:
        logging.warning(f"Skipping bad message: {m!r} ({e})")
        return None

# -------------------------------
# Kafka consumer & producer
# -------------------------------
consumer = KafkaConsumer(
    RAW_TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    group_id=GROUP_ID,
    auto_offset_reset="earliest",
    value_deserializer=safe_json_loads,
    max_poll_records=1,
    fetch_max_wait_ms=100
)

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda m: json.dumps(m).encode("utf-8")
)

logging.info("[Processor] Started...")

batch_count = 0

# -------------------------------
# Processing loop
# -------------------------------
while True:
    try:
        for message in consumer:
            data = message.value
            if not data:
                logging.warning(f"[Processor] Skipping empty/invalid message: {message.value!r}")
                continue

            try:
                # Validate keys
                temp = data.get("temperature")
                humidity = data.get("humidity")
                if temp is None or humidity is None:
                    raise KeyError(f"Missing temperature or humidity in {data}")

                # Compute feels_like
                data["feels_like"] = round(temp - (100 - humidity) / 5, 1)

                # Send to processed topic
                producer.send(PROCESSED_TOPIC, value=data)
                logging.info(f"[Processor] Processed: {data}")

                # Batch flush
                batch_count += 1
                if batch_count >= BATCH_SIZE:
                    producer.flush()
                    batch_count = 0

            except Exception as e:
                logging.error(f"[Processor] Skipping malformed message {data!r}: {e}")
                continue

        # prevent CPU spin if no messages
        time.sleep(0.1)

    except Exception as e:
        logging.error(f"[Processor] Unexpected error: {e}. Retrying in 1s...")
        time.sleep(1)
