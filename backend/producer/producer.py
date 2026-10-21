import time

from kafka.errors import KafkaError

from backend.producer.kafka_client import (
    create_producer,
    KAFKA_BROKER,
    KAFKA_TOPIC,
)
from backend.producer.transaction import generate_transaction

TRANSACTION_INTERVAL = 8

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