import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import paho.mqtt.client as mqtt
from dotenv import load_dotenv

load_dotenv()

# Configuration
HOST = os.getenv("MQTT_HOST", "test.mosquitto.org")
PORT = int(os.getenv("MQTT_PORT", "1883"))
NAMESPACE = os.environ["MQTT_NAMESPACE"]

DATA_DIR = Path(os.getenv("DATA_DIR", "./data"))
FLUSH_INTERVAL_SECONDS = int(
    os.getenv("FLUSH_INTERVAL_SECONDS", "60")
)

DATA_DIR.mkdir(exist_ok=True)

buffer = []
last_flush_time = time.time()


def flush_buffer():
    global buffer
    global last_flush_time

    if not buffer:
        last_flush_time = time.time()
        return

    now = datetime.now(timezone.utc)

    filename = (
        DATA_DIR /
        f"sensors_{now:%Y%m%d_%H%M%S}.jsonl"
    )

    with open(filename, "w", encoding="utf-8") as f:
        for record in buffer:
            f.write(json.dumps(record))
            f.write("\n")

    print(
        f"Wrote {len(buffer)} records to {filename}"
    )

    buffer = []
    last_flush_time = time.time()


def on_connect(
    client,
    userdata,
    flags,
    reason_code,
    properties,
):
    topic = f"{NAMESPACE}/sensor/+"

    client.subscribe(topic)

    print(f"Connected to MQTT broker")
    print(f"Subscribed to {topic}")


def on_message(client, userdata, msg):
    global buffer

    try:
        payload = json.loads(
            msg.payload.decode("utf-8")
        )

        payload["_topic"] = msg.topic
        payload["_received_at"] = (
            datetime.now(timezone.utc)
            .isoformat()
        )

        buffer.append(payload)

    except Exception as ex:
        print(
            f"Failed to process "
            f"message on {msg.topic}: {ex}"
        )


def main():
    global last_flush_time

    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        protocol=mqtt.MQTTv311,
    )

    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(
        HOST,
        PORT,
        keepalive=60,
    )

    client.loop_start()

    try:
        while True:
            elapsed = (
                time.time() - last_flush_time
            )

            if elapsed >= FLUSH_INTERVAL_SECONDS:
                flush_buffer()

            time.sleep(1)

    except KeyboardInterrupt:
        print("Stopping...")

    finally:
        flush_buffer()

        client.loop_stop()
        client.disconnect()

        print("Disconnected")


if __name__ == "__main__":
    main()