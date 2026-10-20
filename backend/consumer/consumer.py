from kafka_client import create_consumer, KAFKA_BROKER, KAFKA_TOPIC
from fraud_rules import detect_fraud


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