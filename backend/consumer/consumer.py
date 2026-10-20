import json
from collections import defaultdict, deque
from datetime import datetime, timedelta

from kafka import KafkaConsumer

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "transactions"
CONSUMER_GROUP = "fraud-detection"

HIGH_VALUE_LIMIT = 10000
TRANSACTION_LIMIT = 4
TIME_WINDOW_SECONDS = 60

customer_transactions = defaultdict(deque)


def create_consumer():
    
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )


def detect_fraud(transaction):
    
    alerts = []

    if transaction["amount"] >= HIGH_VALUE_LIMIT:
        alerts.append("ALTO_VALOR")

    customer_id = transaction["customer_id"]
    transaction_time = datetime.fromisoformat(transaction["timestamp"])

    recent_transactions = customer_transactions[customer_id]
    recent_transactions.append(transaction_time)

    cutoff_time = transaction_time - timedelta(
        seconds=TIME_WINDOW_SECONDS
    )

    while (
        recent_transactions
        and recent_transactions[0] <= cutoff_time
    ):
        recent_transactions.popleft()

    if len(recent_transactions) >= TRANSACTION_LIMIT:
        alerts.append("TEMPO_60s")

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