.PHONY: help up down producer consumer test stats frauds recent

PYTHON = python
DOCKER_COMPOSE = docker compose
POSTGRES_CONTAINER = fraud-stream-postgres

help:
	@echo "Comandos disponiveis:"
	@echo "  make up       - Inicia os servicos Docker"
	@echo "  make down     - Para os servicos Docker"
	@echo "  make producer - Inicia o produtor"
	@echo "  make consumer - Inicia o consumidor"
	@echo "  make test     - Executa os testes de fraude"
	@echo "  make stats    - Mostra o resumo das transacoes"
	@echo "  make frauds   - Lista transacoes suspeitas"
	@echo "  make recent   - Mostra as transacoes recentes"

up:
	$(DOCKER_COMPOSE) up -d

down:
	$(DOCKER_COMPOSE) down

producer:
	$(PYTHON) -m backend.producer.producer

consumer:
	$(PYTHON) -m backend.consumer.consumer

test:
	$(PYTHON) tests/test_fraud_rules.py

stats:
	docker exec -i $(POSTGRES_CONTAINER) psql -U fraud_user -d fraud_stream -c "SELECT COUNT(*) AS total, COUNT(*) FILTER (WHERE is_fraud = TRUE) AS suspeitas, COUNT(*) FILTER (WHERE is_fraud = FALSE) AS normais FROM transactions;"

frauds:
	docker exec -i $(POSTGRES_CONTAINER) psql -U fraud_user -d fraud_stream -c "SELECT customer_id, amount, city, transaction_timestamp, fraud_types FROM transactions WHERE is_fraud = TRUE ORDER BY transaction_timestamp DESC LIMIT 20;"

recent:
	docker exec -i $(POSTGRES_CONTAINER) psql -U fraud_user -d fraud_stream -c "SELECT customer_id, amount, city, transaction_timestamp, is_fraud, fraud_types FROM transactions ORDER BY transaction_timestamp DESC LIMIT 20;"