CREATE TABLE IF NOT EXISTS transactions (
    transaction_id VARCHAR(36) PRIMARY KEY,
    customer_id VARCHAR(10) NOT NULL,
    amount NUMERIC(12, 2) NOT NULL,
    city VARCHAR(100) NOT NULL,
    transaction_timestamp TIMESTAMPTZ NOT NULL,
    is_fraud BOOLEAN NOT NULL DEFAULT FALSE,
    fraud_types TEXT[] NOT NULL DEFAULT '{}'
);