import random
import uuid
from datetime import datetime, timezone

CUSTOMERS = [f"C{i:03d}" for i in range(1, 301)]

CITIES = [
    "Juazeiro do Norte",
    "Barbalha",
    "Crato",
    "Caririaçu",
    "Missão Velha",
]


def generate_transaction():
    return {
        "transaction_id": str(uuid.uuid4()),
        "customer_id": random.choice(CUSTOMERS),
        "amount": round(random.uniform(10, 15000), 2),
        "city": random.choice(CITIES),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }