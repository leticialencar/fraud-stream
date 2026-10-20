import json

from kafka import KafkaProducer

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "transactions"


def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        retries=5,
    )