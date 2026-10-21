from backend.consumer.kafka_client import (
    create_consumer,
    KAFKA_BROKER,
    KAFKA_TOPIC,
)
from backend.consumer.fraud_rules import detect_fraud
from database.database import save_transaction


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

            try:
                save_transaction(transaction, alerts)
                print("Transaction saved to database.")
            except Exception as error:
                print(f"Database error: {error}")
                continue

            if alerts:
                for alert in alerts:
                    print(
                        f"FRAUD ALERT: {alert} | "
                        f"transaction_id={transaction['transaction_id']} | "
                        f"customer={transaction['customer_id']} | "
                        f"amount=R${transaction['amount']:.2f}"
                    )
            else:
                print("No fraud detected.")

            print()

    except KeyboardInterrupt:
        print("\nStopping transaction consumer.")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()
