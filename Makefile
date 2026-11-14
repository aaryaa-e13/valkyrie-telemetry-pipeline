# Project VALKYRIE - developer commands.
# Run `make help` to list targets.

SHELL := /bin/bash
COMPOSE := docker compose
TOPIC ?= game-events
KAFKA_BIN := /opt/kafka/bin
BOOTSTRAP_INTERNAL := kafka:29092

.DEFAULT_GOAL := help
.PHONY: help env up down ps logs topics describe-topic

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
