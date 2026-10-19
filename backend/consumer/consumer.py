import json

from kafka import KafkaConsumer

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "transactions"
CONSUMER_GROUP = "fraud-detection"

HIGH_VALUE_LIMIT = 10000


def create_consumer():
    """Create a connection to the Kafka topic."""

    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )


def detect_fraud(transaction):
    """Check whether a transaction matches a fraud rule."""

    alerts = []

    if transaction["amount"] >= HIGH_VALUE_LIMIT:
        alerts.append("ALTO_VALOR")

    return alerts


def main():
    consumer = create_consumer()

    print("Transaction consumer started.")
    print(f"Kafka broker: {KAFKA_BROKER}")
    print(f"Kafka topic: {KAFKA_TOPIC}")
    print("Waiting for transactions...\n")

    try:
        for message in consumer:
            transaction = message.value

            print(
                f"Transaction received: "
                f"id={transaction['transaction_id']}, "
                f"customer={transaction['customer_id']}, "
                f"amount=R${transaction['amount']:.2f}, "
                f"city={transaction['city']}"
            )

            alerts = detect_fraud(transaction)

            for alert in alerts:
                print(
                    f"FRAUD ALERT: {alert} | "
                    f"transaction_id={transaction['transaction_id']} | "
                    f"customer={transaction['customer_id']} | "
                    f"amount=R${transaction['amount']:.2f}"
                )

    except KeyboardInterrupt:
        print("\nStopping transaction consumer.")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()