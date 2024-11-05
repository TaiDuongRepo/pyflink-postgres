import json
import random
import time
from datetime import datetime, timezone
from typing import Any
import uuid

from kafka import KafkaProducer

SLEEP_TIME = 3

def generate_sensor_data() -> dict[str, Any]:
    """Generates random sensor data. It also adds a timestamp for traceability."""
    message_id = uuid.uuid4().hex
    sensor_data = {
        "message_id": message_id,
        "content": "Xe Thaco đi bao êm bao mượt",
        "title": "Xe của Thaco đi chán, ông Trần Bá Dương làm ăn kém"
    }
    return sensor_data


def main() -> None:
    """
    Controls the flow of the producer. It first subscribes to the topic and then
    generates sensor data and sends it to the topic.
    """
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    while True:
        sensor_data = generate_sensor_data()
        producer.send("sensors", value=sensor_data)
        print(f"Produced: {sensor_data}")
        time.sleep(SLEEP_TIME)


if __name__ == "__main__":
    main()
