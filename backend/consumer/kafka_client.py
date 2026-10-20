import json

from kafka import KafkaConsumer

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "transactions"
CONSUMER_GROUP = "fraud-detection"


def create_consumer():
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers = KAFKA_BROKER,
        group_id = CONSUMER_GROUP,
        auto_offset_reset = "earliest",
        enable_auto_commit = True,
        value_deserializer = lambda value: json.loads(
            value.decode("utf-8")
        ),
    )