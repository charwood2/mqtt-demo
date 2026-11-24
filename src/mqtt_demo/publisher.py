import json
import logging
import os
import random
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
from dotenv import load_dotenv

load_dotenv()

# Configuration
HOST = os.getenv("MQTT_HOST", "test.mosquitto.org")
PORT = int(os.getenv("MQTT_PORT", "1883"))
NAMESPACE = os.environ["MQTT_NAMESPACE"]

NUMBER_OF_SENSORS = int(os.environ["NUMBER_OF_SENSORS"])
NUMBER_OF_SAMPLES = int(os.environ["NUMBER_OF_SAMPLES"])

PUBLISH_INTERVAL = float(os.getenv("PUBLISH_INTERVAL", "1"))

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)


class Sensor:
    def __init__(self, sensor_id: int):
        self.sensor_id = sensor_id
        self.sequence_no = 0
        self.cumulative_value = 0.0
        self.timestamp = self._current_timestamp()

    @staticmethod
    def _current_timestamp() -> str:
        return datetime.now(timezone.utc).isoformat()

    def increment(self) -> None:
        self.sequence_no += 1
        self.timestamp = self._current_timestamp()
        self.cumulative_value += random.random()

    def get_state(self) -> str:
        return json.dumps(
            {
                "sensor_id": self.sensor_id,
                "sequence_no": self.sequence_no,
                "timestamp": self.timestamp,
                "cumulative_value": round(self.cumulative_value, 4),
            }
        )


def create_client() -> mqtt.Client:
    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"sensor-publisher-{random.randint(1000, 9999)}",
        protocol=mqtt.MQTTv311,
    )

    try:
        client.connect(HOST, PORT, keepalive=60)
        logging.info("Connected to MQTT broker %s:%s", HOST, PORT)
    except Exception as ex:
        logging.exception("Failed to connect to MQTT broker")
        raise ex

    return client


def main() -> None:
    client = create_client()
    client.loop_start()

    sensors = [Sensor(sensor_id=i) for i in range(NUMBER_OF_SENSORS)]

    try:
        for _ in range(NUMBER_OF_SAMPLES):
            for sensor in sensors:
                sensor.increment()

                topic = f"{NAMESPACE}/sensor/sensor_{sensor.sensor_id}"
                payload = sensor.get_state()

                info = client.publish(
                    topic=topic,
                    payload=payload,
                    qos=0,
                    retain=False,
                )

                info.wait_for_publish(timeout=5)

                logging.info(
                    "Published to %s: %s",
                    topic,
                    payload,
                )

            time.sleep(PUBLISH_INTERVAL)

            time.sleep(1)

    except KeyboardInterrupt:
        logging.info("Interrupted by user")

    finally:
        client.loop_stop()
        client.disconnect()
        logging.info("Disconnected from MQTT broker")


if __name__ == "__main__":
    main()