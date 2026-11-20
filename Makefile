# Project VALKYRIE - developer commands.
# Run `make help` to list targets.

SHELL := /bin/bash
COMPOSE := docker compose
TOPIC ?= game-events
KAFKA_BIN := /opt/kafka/bin
BOOTSTRAP_INTERNAL := kafka:29092
VENV := .venv
PY := $(VENV)/bin/python
# uv is found on PATH. Override with `make venv UV=/path/to/uv` if needed.
UV ?= $(shell command -v uv 2>/dev/null)
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
	spark-stop spark-logs verify test-spark test-spark-fast demo smoke smoke-dupes e2e-images smoke-restart smoke-late smoke-faults faults-config-check \
	delta-compat jars-lock

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
	@if [ -z "$(UV)" ]; then \
		echo "uv not found on PATH. Install it (https://docs.astral.sh/uv/) or pass UV=/path/to/uv." >&2; \
		exit 1; \
	fi
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

sim-late: dirs ## Realtime pace, 30% deliberately late events (very late = 120-180 s), ~3.5 min (fresh seed unless SEED=)
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

spark-build: dirs ## Build the Spark image (pyspark 4.1.4 + Kafka connector + Delta)
	$(COMPOSE) --profile spark build spark

spark-up: dirs ## Start the streaming job Kafka -> bronze -> silver + late Delta (background)
	$(COMPOSE) --profile spark up -d spark

spark-stop: ## Stop the streaming job gracefully (checkpoint keeps its position)
	$(COMPOSE) --profile spark stop spark

spark-logs: ## Follow streaming job logs
	$(COMPOSE) --profile spark logs -f spark

spark-image: ## Rebuild the Spark image quietly if code changed (cached: a few seconds)
	@$(COMPOSE) --profile spark build --quiet spark

# verify depends on spark-image: the manifest reader is baked into the image, so a
# stale image would verify with old code (it once rejected v3 manifests; WHAT_BROKE).
verify: dirs spark-image ## Compare bronze+silver with a manifest (MANIFEST=... [LAYER=bronze|silver|all])
	@test -n "$(MANIFEST)" || (echo "usage: make verify MANIFEST=manifests/run_X.json" && exit 2)
	$(COMPOSE) --profile tools run --rm -T spark-tools \
		python -m streaming.verify_manifest --manifest /app/$(MANIFEST) --layer $(or $(LAYER),all) \
		$(VERIFY_ARGS)

test-spark: dirs ## Run all Spark tests in the Spark image (SPARK_TESTS=path to narrow)
	$(COMPOSE) --profile spark run --rm --no-deps -T -v ./:/app:ro spark \
		python -m pytest $(or $(SPARK_TESTS),tests/spark) -q -p no:cacheprovider $(SPARK_TEST_ARGS)

test-spark-fast: dirs ## Quick Spark subset for the dev loop (marker spark_fast, < 5 min)
	$(COMPOSE) --profile spark run --rm --no-deps -T -v ./:/app:ro spark \
		python -m pytest tests/spark -q -p no:cacheprovider -m spark_fast --durations=10

# Delta compatibility gate (Phase 2 slice 1, DECISIONS D2.1): checks C1-C14 in the
# container's /tmp, prints one JSON report, exits 0 (PASS) or 1. Runs in spark-tools
# (./data read-only), so it can run next to the streaming job.
delta-compat: dirs spark-image ## Delta compatibility checks C1-C14 (JSON report, PASS/FAIL)
	$(COMPOSE) --profile tools run --rm -T spark-tools \
		python -m streaming.delta_compat_check $(DELTA_COMPAT_ARGS)

# Regenerate the jar SHA-256 lock ON PURPOSE (after changing a fetched package).
# Builds a separate image tag without lock verification, hashes every added jar,
# cross-checks it against Maven Central's .sha1 over HTTPS, and only then replaces
# docker/spark/jars.sha256. Review the result with `git diff docker/spark/jars.sha256`.
JARS_LOCK := docker/spark/jars.sha256
jars-lock: ## Regenerate docker/spark/jars.sha256 (cross-checked with Maven Central); review git diff
	docker build -q -f docker/spark/Dockerfile --build-arg JARS_LOCK_VERIFY=0 \
		-t valkyrie-spark:jars-lock . > /dev/null
	docker run --rm --entrypoint bash valkyrie-spark:jars-lock -c \
		'python /opt/valkyrie/jars_lock.py generate /opt/valkyrie/jars-*.txt --jars-dir \
		"$$(python -c "import os, pyspark; print(os.path.dirname(pyspark.__file__))")/jars"' \
		> $(JARS_LOCK).new || { rm -f $(JARS_LOCK).new; exit 1; }
	mv $(JARS_LOCK).new $(JARS_LOCK)
	@git diff --stat -- $(JARS_LOCK) || true

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

smoke-dupes: dirs up e2e-images ## Same seed sent twice: bronze 2 rows/id, silver 1 (first send)
	$(PY) -m e2e.smoke_dupes $(E2E_ARGS)

smoke-restart: dirs up e2e-images ## Graceful stop/restart: no re-read, no skip (~3-7 min)
	$(PY) -m e2e.smoke_restart $(E2E_ARGS)

smoke-late: dirs up e2e-images ## Realtime run with late events, judged by R1-R6 (~7-9 min)
	$(PY) -m e2e.smoke_late $(E2E_ARGS)

smoke-faults: dirs up e2e-images ## SIGKILL silver at each write boundary + bronze, restart, judge (~45 min; one point ~10)
	$(PY) -m e2e.smoke_faults $(E2E_ARGS)

faults-config-check: ## Default compose config must not mention the fault hooks: prints 0
	@n=$$($(COMPOSE) --profile spark --profile tools --profile sim config | grep -c VALKYRIE_FAULT || true); \
		echo $$n; test "$$n" = "0"
