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
# For targets that must not reuse a seed (sim-realtime, sim-late): use SEED if you
# passed one (make sim-late SEED=7), otherwise a fresh one (epoch milliseconds,
# generated in Python like `make smoke` does).
FRESH_SEED = $(if $(filter file,$(origin SEED)),$(shell $(PY) -c "import time; print(time.time_ns() // 1_000_000)"),$(SEED))

.DEFAULT_GOAL := help

.PHONY: dirs
dirs: ## Create bind-mount folders owned by you (Docker would create them as root)
	mkdir -p data data/status manifests

.PHONY: help env up down ps logs topics describe-topic venv sim-dry sim-local sim-tiny sim \
	sim-realtime sim-late timing-check consume test-unit lint spark-build spark-image spark-up \
	spark-stop spark-logs verify test-spark demo smoke e2e-images smoke-restart

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

sim-tiny: dirs ## Send a tiny run (~10-14 events) from the laptop via localhost:9092 (SEED=42)
	$(PY) -m simulator.main --config simulator/config.tiny.yaml --seed $(SEED)

sim-local: dirs ## Send a default run from the laptop via localhost:9092 (SEED=42)
	$(PY) -m simulator.main --config simulator/config.yaml --seed $(SEED)

sim: dirs ## Send a tiny run from the simulator container via kafka:29092 (SEED=42)
	$(COMPOSE) --profile sim run --rm --build simulator --config simulator/config.tiny.yaml --seed $(SEED)

sim-realtime: dirs ## Realtime pace: tiny run, game time 10x faster, ~15 s (fresh seed unless SEED=)
	$(PY) -m simulator.main --config simulator/config.tiny.yaml --seed $(FRESH_SEED) \
		--pace realtime --time-scale 10

sim-late: dirs ## Realtime pace with 30% deliberately late events, up to ~2 min (fresh seed unless SEED=)
	$(PY) -m simulator.main --config simulator/config.tiny.yaml --seed $(FRESH_SEED) \
		--pace realtime --time-scale 10 --late-fraction 0.3

timing-check: ## Read a run back from Kafka: was any event sent before its event_time? (MANIFEST=...)
	@test -n "$(MANIFEST)" || (echo "usage: make timing-check MANIFEST=manifests/run_X.json" && exit 2)
	@$(PY) -m e2e.timing_check --manifest $(MANIFEST) $(TIMING_ARGS)

test-unit: ## Run unit tests (no Docker needed)
	$(PY) -m pytest tests/unit -q

lint: ## Run ruff
	$(VENV)/bin/ruff check .

# --- Spark (container) --------------------------------------------------------

spark-build: dirs ## Build the Spark image (pyspark 4.1.4 + Kafka connector)
	$(COMPOSE) --profile spark build spark

spark-up: dirs ## Start the streaming job Kafka -> raw Parquet (background)
	$(COMPOSE) --profile spark up -d spark

spark-stop: ## Stop the streaming job gracefully (checkpoint keeps its position)
	$(COMPOSE) --profile spark stop spark

spark-logs: ## Follow streaming job logs
	$(COMPOSE) --profile spark logs -f spark

spark-image: ## Rebuild the Spark image quietly if code changed (cached: a few seconds)
	@$(COMPOSE) --profile spark build --quiet spark

# verify depends on spark-image: the manifest reader is baked into the image, so a
# stale image would verify with old code (it once rejected v3 manifests; WHAT_BROKE).
verify: dirs spark-image ## Compare raw Parquet with a run manifest (MANIFEST=manifests/run_X.json)
	@test -n "$(MANIFEST)" || (echo "usage: make verify MANIFEST=manifests/run_X.json" && exit 2)
	$(COMPOSE) --profile tools run --rm -T spark-tools \
		python -m streaming.verify_manifest --manifest /app/$(MANIFEST)

test-spark: dirs ## Run Spark transformation tests inside the Spark image
	$(COMPOSE) --profile spark run --rm --no-deps -T -v ./:/app:ro spark \
		python -m pytest tests/spark -q -p no:cacheprovider

# --- End-to-end (laptop orchestrator, see e2e/orchestrator.py) -----------------
# Exit codes of the orchestrator: 0 pass, 1 verify failed, 2 setup problem,
# 3 Spark did not catch up, 4 contiguity violation. make itself then reports
# "Error N" and exits 2; the real code is in the SUMMARY line.
# Extra flags: make smoke E2E_ARGS="--timeout 5"

e2e-images: ## Build the simulator and Spark images quietly (used by demo/smoke)
	$(COMPOSE) --profile sim --profile spark build --quiet simulator spark

demo: dirs up e2e-images ## Full run with explanations: simulator -> Kafka -> Spark -> verify
	$(PY) -m e2e.orchestrator --mode demo $(E2E_ARGS)

smoke: dirs up e2e-images ## Quick end-to-end check (tiny run), one JSON SUMMARY line
	$(PY) -m e2e.orchestrator --mode smoke $(E2E_ARGS)

smoke-restart: dirs up e2e-images ## Graceful stop/restart: no re-read, no skip (~1-2 min)
	$(PY) -m e2e.smoke_restart $(E2E_ARGS)
