import os

import psycopg

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://fraud_user:dev_password_change_me@localhost:5433/fraud_stream"
)


def create_connection():
    return psycopg.connect(DATABASE_URL)


def save_transaction(transaction, alerts):
    query = """
        INSERT INTO transactions (
            transaction_id,
            customer_id,
            amount,
            city,
            transaction_timestamp,
            is_fraud,
            fraud_types
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (transaction_id) DO NOTHING
    """

    with create_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                query,
                (
                    transaction["transaction_id"],
                    transaction["customer_id"],
                    transaction["amount"],
                    transaction["city"],
                    transaction["timestamp"],
                    bool(alerts),
                    alerts,
                ),
            )