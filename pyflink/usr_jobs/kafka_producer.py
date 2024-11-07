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
    sensor_data = {
        "url": "https://www.youtube.com/watch?v=btUbLgsftKc",
        "address": None,
        "brand": "thaco auto",
        "comment_count": 2,
        "comments": [
            {
                "created_at": "2023-04-16T02:47:02Z",
                "reply_to": None,
                "user_name": "@hoaikhangphan6350",
                "nickname": "@hoaikhangphan6350",
                "user_id": "UC3SJzlHeA43kl0qGhUSji4w",
                "email": None,
                "phone": None,
                "address": None,
                "title": None,
                "content": "Bn 1 chiếc ạ",
                "reaction": {
                    "like": 0,
                    "love": None,
                    "haha": None,
                    "wow": None,
                    "sad": None,
                    "angry": None,
                    "other": None,
                },
                "id": "Ugw1V0Dr0ulPjl-yiJR4AaABAg",
            },
            {
                "created_at": "2022-12-22T12:24:23Z",
                "reply_to": None,
                "user_name": "@LongThanh-zw8zv",
                "nickname": "@LongThanh-zw8zv",
                "user_id": "UC3SJzlHeA43kl0qGhUSji4w",
                "email": None,
                "phone": None,
                "address": None,
                "title": None,
                "content": "Giá lăn bánh bao nhiêu ak",
                "reaction": {
                    "like": 0,
                    "love": None,
                    "haha": None,
                    "wow": None,
                    "sad": None,
                    "angry": None,
                    "other": None,
                },
                "id": "Ugw3mJ7-pp0aXaeckxl4AaABAg",
            },
        ],
        "description": "THACO BLUE SKY 120S -XE BUS GHẾ NGỒI HIỆN ĐẠI, SANG TRỌNG\nDòng xe bus ghế ngồi cao cấp (28 chỗ & 47 chỗ), kiểu dáng Universe hiện đại, nội thất sang trọng",
        "email": None,
        "favorite_count": 0,
        "id": "btUbLgsftKc",
        "keyword": ["#thaco"],
        "media_channel": "youtube",
        "phone": None,
        "reaction": {
            "like": 50,
            "love": None,
            "haha": None,
            "wow": None,
            "sad": None,
            "angry": None,
            "other": None,
        },
        "share": None,
        "social_type": "youtube",
        "title": "Xe khách 47 ghế cao cấp của Thaco. Trải nghiệm và đánh giá nhanh xe Bluesky TB120S |LH: 0911 102 664",
        "user_id": "UC3SJzlHeA43kl0qGhUSji4w",
        "user_name": "Mẫn Xe Khách",
        "view": 7054,
        "created_at": "2022-12-11T08:05:44Z",
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
