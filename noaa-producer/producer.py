import time
import json
import logging
import requests
from kafka import KafkaProducer

# -------------------------------
# Configuration
# -------------------------------
BOOTSTRAP_SERVERS = "kafka:9092"
TOPIC = "weather.raw"
SEND_INTERVAL = 60  # NOAA updates ~1/min
LOG_LEVEL = logging.INFO

NOAA_STATION = "CYYZ"  # Toronto Pearson (change if needed)
NOAA_URL = f"https://api.weather.gov/stations/{NOAA_STATION}/observations/latest"

HEADERS = {
    # REQUIRED by NOAA or you'll get 403 / throttled
    "User-Agent": "weather-kafka-producer (bryan@example.com)",
    "Accept": "application/geo+json"
}

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
    retries=3,
    linger_ms=0
)

# -------------------------------
# Helpers
# -------------------------------
def c_to_f(c):
    return round((c * 9 / 5) + 32, 1) if c is not None else None

def safe_value(field):
    return field.get("value") if field else None

def fetch_noaa_weather():
    resp = requests.get(NOAA_URL, headers=HEADERS, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    props = data.get("properties", {})

    return {
        "station": NOAA_STATION,
        "timestamp": props.get("timestamp"),

        # Temperatures
        "temperature_c": safe_value(props.get("temperature")),
        "temperature_f": c_to_f(safe_value(props.get("temperature"))),

        # Humidity & pressure
        "humidity": safe_value(props.get("relativeHumidity")),
        "pressure_pa": safe_value(props.get("barometricPressure")),

        # Wind
        "wind_speed_mps": safe_value(props.get("windSpeed")),
        "wind_direction_deg": safe_value(props.get("windDirection")),

        # Visibility
        "visibility_m": safe_value(props.get("visibility")),

        # Raw text summary (optional but useful)
        "description": props.get("textDescription"),

        # Metadata
        "source": "noaa",
    }

# -------------------------------
# Callbacks
# -------------------------------
def on_send_success(record_metadata):
    logging.info(
        f"[Producer] Sent → topic={record_metadata.topic}, "
        f"partition={record_metadata.partition}, offset={record_metadata.offset}"
    )

def on_send_error(excp):
    logging.error("[Producer] Kafka send failed", exc_info=excp)

# -------------------------------
# Main loop
# -------------------------------
logging.info("[Producer] NOAA weather producer started")

while True:
    try:
        weather = fetch_noaa_weather()

        # Basic validation to avoid crashing downstream processors
        if weather["temperature_c"] is None:
            logging.warning("[Producer] Incomplete NOAA payload, skipping")
        else:
            producer.send(TOPIC, value=weather) \
                    .add_callback(on_send_success) \
                    .add_errback(on_send_error)

            producer.flush()
            logging.info(f"[Producer] Payload: {weather}")

    except requests.RequestException as e:
        logging.error("[Producer] NOAA fetch failed", exc_info=True)

    except Exception as e:
        logging.error("[Producer] Unexpected error", exc_info=True)

    time.sleep(SEND_INTERVAL)
