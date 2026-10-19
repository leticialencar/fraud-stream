import json
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "transactions"
TRANSACTION_INTERVAL = 3

CUSTOMERS = ["C001", "C002", "C003", "C004", "C005"]

CITIES = [
    "Juazeiro do Norte",
    "Barbalha",
    "Crato",
    "Caririaçu",
    "Missão Velha",
]


def generate_transaction():
    """Generate a simulated financial transaction."""

    return {
        "transaction_id": str(uuid.uuid4()),
        "customer_id": random.choice(CUSTOMERS),
        "amount": round(random.uniform(10, 15000), 2),
        "city": random.choice(CITIES),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def create_producer():
    """Create a connection to the Kafka broker."""

    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        retries=5,
    )


def main():
    producer = None

    try:
        producer = create_producer()

        print("Transaction producer started.")
        print(f"Kafka broker: {KAFKA_BROKER}")
        print(f"Kafka topic: {KAFKA_TOPIC}")
        print(
            f"Generating transactions every "
            f"{TRANSACTION_INTERVAL} seconds...\n"
        )

        while True:
            transaction = generate_transaction()

            try:
                future = producer.send(
                    KAFKA_TOPIC,
                    value=transaction,
                )
                future.get(timeout=10)

                print(
                    f"Transaction sent: "
                    f"id={transaction['transaction_id']}, "
                    f"customer={transaction['customer_id']}, "
                    f"amount=R${transaction['amount']:.2f}, "
                    f"city={transaction['city']}"
                )

            except KafkaError as error:
                print(f"Error sending transaction: {error}")

            time.sleep(TRANSACTION_INTERVAL)

    except KeyboardInterrupt:
        print("\nStopping transaction producer.")

    except KafkaError as error:
        print(f"Kafka error: {error}")

    finally:
        if producer is not None:
            producer.close()


if __name__ == "__main__":
    main()