import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from backend.consumer.fraud_rules import (
    detect_fraud,
    customer_transactions,
)

def create_transaction(customer_id, amount, city, seconds_after=0):
    transaction_time = start_time + timedelta(seconds=seconds_after)

    return {
        "transaction_id": f"TEST-{customer_id}-{seconds_after}",
        "customer_id": customer_id,
        "amount": amount,
        "city": city,
        "timestamp": transaction_time.isoformat(),
    }


def run_test(test_name, transactions, expected_alerts):
    customer_transactions.clear()

    alerts = []

    for transaction in transactions:
        alerts = detect_fraud(transaction)

    print(f"\\nTeste: {test_name}")
    print(f"Alertas encontrados: {alerts}")
    print(f"Alertas esperados:  {expected_alerts}")

    if set(alerts) == set(expected_alerts):
        print("RESULTADO: PASSOU")
    else:
        print("RESULTADO: FALHOU")


start_time = datetime.now(timezone.utc)

run_test(
    "Alto valor",
    [
        create_transaction("C001", 12000, "Crato"),
    ],
    ["ALTO_VALOR"],
)

run_test(
    "Quatro transações em menos de 60 segundos",
    [
        create_transaction("C002", 100, "Crato", 0),
        create_transaction("C002", 200, "Crato", 10),
        create_transaction("C002", 300, "Crato", 20),
        create_transaction("C002", 400, "Crato", 30),
    ],
    ["TEMPO_60s"],
)

run_test(
    "Transações em cidades diferentes em menos de 10 minutos",
    [
        create_transaction("C003", 100, "Crato", 0),
        create_transaction("C003", 200, "Juazeiro do Norte", 60),
    ],
    ["GEO_10m"],
)

run_test(
    "Transação normal",
    [
        create_transaction("C004", 100, "Barbalha"),
    ],
    [],
)
