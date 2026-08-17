.DEFAULT_GOAL := help
SHELL := /bin/bash
COMPOSE := docker compose

.PHONY: help init build up down restart ps logs mysql psql q qm test integration lint dag-test clean nuke

help:  ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
	  | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

init:  ## Create .env with freshly generated secrets (does not overwrite)
	@if [ -f .env ]; then echo ".env exists, leaving it"; else \
	  sed -e "s|^AIRFLOW_UID=.*|AIRFLOW_UID=$$(id -u)|" \
	      -e "s|^FERNET_KEY=.*|FERNET_KEY=$$(openssl rand -base64 32 | tr '+/' '-_')|" \
	      -e "s|^AIRFLOW__API_AUTH__JWT_SECRET=.*|AIRFLOW__API_AUTH__JWT_SECRET=$$(openssl rand -hex 32)|" \
	      -e "s|^MYSQL_PASSWORD=.*|MYSQL_PASSWORD=$$(openssl rand -hex 16)|" \
	      -e "s|^MYSQL_ROOT_PASSWORD=.*|MYSQL_ROOT_PASSWORD=$$(openssl rand -hex 16)|" \
	      -e "s|^POSTGRES_PASSWORD=.*|POSTGRES_PASSWORD=$$(openssl rand -hex 16)|" \
	      .env.example > .env && chmod 600 .env && \
	  echo "created .env with generated secrets"; fi

build:  ## Build the extended Airflow image
	$(COMPOSE) build

up:  ## Start the whole stack in the background
	$(COMPOSE) up -d
	@echo "Airflow UI -> http://localhost:$${AIRFLOW_UI_PORT:-8080}"

down:  ## Stop the stack, keep the data volumes
	$(COMPOSE) down

restart: down up  ## Stop then start

ps:  ## Show container status and health
	$(COMPOSE) ps

logs:  ## Tail logs (make logs S=airflow-scheduler for one service)
	$(COMPOSE) logs -f $(S)

mysql:  ## Open a MySQL shell on the staging database
	$(COMPOSE) exec mysql-staging sh -c 'exec mysql -u"$$MYSQL_USER" -p"$$MYSQL_PASSWORD" "$$MYSQL_DATABASE"'

psql:  ## Open a psql shell on the analytics database
	$(COMPOSE) exec postgres-analytics sh -c 'exec psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB"'

q:  ## One-off query on analytics: make q SQL="SELECT COUNT(*) FROM fct_flights"
	@$(COMPOSE) exec -T postgres-analytics sh -c 'exec psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB" -c "$(SQL)"'

qm:  ## One-off query on staging: make qm SQL="SELECT COUNT(*) FROM stg_flights"
	@$(COMPOSE) exec -T mysql-staging sh -c 'exec mysql -t -u"$$MYSQL_USER" -p"$$MYSQL_PASSWORD" "$$MYSQL_DATABASE" -e "$(SQL)"' 2>/dev/null

test:  ## Run the test suite inside the Airflow image
	$(COMPOSE) run --rm --no-deps airflow-cli bash -c 'cd /opt/airflow && python -m pytest tests -q'

integration:  ## End-to-end: run the real DAG against the corrupted fixture and assert
	./scripts/integration_test.sh

lint:  ## Lint and format-check (same rules CI enforces)
	docker run --rm -v "$$PWD":/w -w /w python:3.13-slim sh -c \
	  'pip install -q ruff && ruff check . && ruff format --check .'

dag-test:  ## Parse-check every DAG (catches import errors before the scheduler does)
	$(COMPOSE) run --rm --no-deps airflow-cli bash -c 'airflow dags list-import-errors'

clean:  ## Stop and DELETE all data volumes (databases wiped)
	$(COMPOSE) down -v

nuke: clean  ## clean + remove the built image
	-docker rmi flight-price/airflow:3.3.1
