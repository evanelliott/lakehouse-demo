# --- Import environment variables from .env ---
include .env
export

.PHONY: help up down restart status logs clean seed health jupyter

help: ## Show this help message
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

# --- Docker Operations ---
up: ## Build and start the Lakehouse stack
	docker compose up --build -d
	@echo "Stack is starting... Use 'make health' to check status."

down: ## Stop and remove all containers
	docker compose down

restart: down up ## Full restart of the stack

status: ## Check the status of all containers
	docker compose ps

logs: ## Tail all container logs
	docker compose logs -f

# --- Data & Seeds ---
seed: ## Run the initial schema and table creation script
	python3 scripts/seed_data.py

mock-data: ## Generate and upload fake football match events
	python3 scripts/generate_mock_data.py

clean: ## DANGER: Reset the entire lake (wipes data, logs, and volumes)
	docker compose down -v
	rm -rf ./data/minio/*
	rm -rf ./data/airflow_db/*
	rm -rf ./airflow/logs/*
	@echo "Lakehouse has been purged."

# --- Health & Entrypoints ---
health: ## Run the health check script to verify services
	bash scripts/check_health.sh

urls: ## Show the access URLs for the demo
	@echo "------------------------------------------------"
	@echo "Trino UI:      http://localhost:$(TRINO_PORT)"
	@echo "MinIO Console: http://localhost:$(MINIO_CONSOLE_PORT)"
	@echo "Airflow UI:    http://localhost:$(AIRFLOW_PORT)"
	@echo "Jupyter:       http://localhost:$(JUPYTER_PORT) (Token: $(JUPYTER_TOKEN))"
	@echo "------------------------------------------------"
