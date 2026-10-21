from collections import defaultdict, deque
from datetime import datetime, timedelta

HIGH_VALUE_LIMIT = 10000
TRANSACTION_LIMIT = 4
TIME_WINDOW_SECONDS = 60
GEO_WINDOW_MINUTES = 10

customer_transactions = defaultdict(deque)


def detect_fraud(transaction):
    alerts = []

    amount = float(transaction["amount"])
    customer_id = transaction["customer_id"]
    city = transaction["city"]

    transaction_time = datetime.fromisoformat(
        transaction["timestamp"]
    )

    if amount >= HIGH_VALUE_LIMIT:
        alerts.append("ALTO_VALOR")

    history = customer_transactions[customer_id]

    geo_cutoff = transaction_time - timedelta(
        minutes=GEO_WINDOW_MINUTES
    )

    while history and history[0]["timestamp"] < geo_cutoff:
        history.popleft()

    time_cutoff = transaction_time - timedelta(
        seconds=TIME_WINDOW_SECONDS
    )

    recent_transactions = [
        item
        for item in history
        if time_cutoff < item["timestamp"] < transaction_time
    ]

    if len(recent_transactions) + 1 >= TRANSACTION_LIMIT:
        alerts.append("TEMPO_60s")

    recent_geo_transactions = [
        item
        for item in history
        if geo_cutoff <= item["timestamp"] < transaction_time
    ]

    if any(
        item["city"] != city
        for item in recent_geo_transactions
    ):
        alerts.append("GEO_10m")

    history.append({
        "timestamp": transaction_time,
        "city": city,
    })

    return alerts