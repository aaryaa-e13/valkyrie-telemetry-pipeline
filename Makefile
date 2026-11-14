# Project VALKYRIE - developer commands.
# Run `make help` to list targets.

SHELL := /bin/bash
COMPOSE := docker compose
TOPIC ?= game-events
KAFKA_BIN := /opt/kafka/bin
BOOTSTRAP_INTERNAL := kafka:29092
VENV := .venv
PY := $(VENV)/bin/python
UV := $(shell command -v uv 2>/dev/null || echo $(HOME)/.local/bin/uv)
SEED ?= 42

.DEFAULT_GOAL := help
.PHONY: help env up down ps logs topics describe-topic venv sim-dry sim-local sim-tiny sim consume test-unit lint

help: ## List available targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-16s %s\n", $$1, $$2}'

env: ## Create .env from .env.example if it does not exist
	@test -f .env || (cp .env.example .env && echo "created .env")

up: env ## Start core services (Kafka) and create topics
	$(COMPOSE) up -d kafka
	$(COMPOSE) run --rm kafka-init

down: ## Stop all services (keeps data volumes)
	$(COMPOSE) --profile '*' down

ps: ## Show service status
	$(COMPOSE) ps -a

logs: ## Follow Kafka logs
	$(COMPOSE) logs -f kafka

topics: ## List topics
	$(COMPOSE) exec kafka $(KAFKA_BIN)/kafka-topics.sh --bootstrap-server $(BOOTSTRAP_INTERNAL) --list

describe-topic: ## Describe a topic (TOPIC=game-events)
	$(COMPOSE) exec kafka $(KAFKA_BIN)/kafka-topics.sh --bootstrap-server $(BOOTSTRAP_INTERNAL) --describe --topic $(TOPIC)

consume: ## Print all messages with partition, offset and key (exits after 8 s idle)
	$(COMPOSE) exec -T kafka $(KAFKA_BIN)/kafka-console-consumer.sh \
		--bootstrap-server $(BOOTSTRAP_INTERNAL) --topic $(TOPIC) --from-beginning --timeout-ms 8000 \
		--formatter-property print.partition=true --formatter-property print.offset=true \
		--formatter-property print.key=true --formatter-property key.separator=' | '

# --- Python (laptop) ----------------------------------------------------------

venv: ## Create .venv (Python 3.11 via uv) and install pinned dev dependencies
	$(UV) venv --python 3.11 $(VENV)
	$(UV) pip install --python $(PY) -r requirements/dev.txt

sim-dry: ## Print tiny-run events without Kafka (SEED=42)
	$(PY) -m simulator.main --config simulator/config.tiny.yaml --seed $(SEED) --dry-run

sim-tiny: ## Send a tiny run (~10-14 events) from the laptop via localhost:9092 (SEED=42)
	$(PY) -m simulator.main --config simulator/config.tiny.yaml --seed $(SEED)

sim-local: ## Send a default run from the laptop via localhost:9092 (SEED=42)
	$(PY) -m simulator.main --config simulator/config.yaml --seed $(SEED)

sim: ## Send a tiny run from the simulator container via kafka:29092 (SEED=42)
	$(COMPOSE) --profile sim run --rm --build simulator --config simulator/config.tiny.yaml --seed $(SEED)

test-unit: ## Run unit tests (no Docker needed)
	$(PY) -m pytest tests/unit -q

lint: ## Run ruff
	$(VENV)/bin/ruff check .
