import random
import uuid
from datetime import datetime, timezone

CUSTOMERS = ["C001", "C002", "C003", "C004", "C005"]

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