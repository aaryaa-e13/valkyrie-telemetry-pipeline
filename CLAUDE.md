# Project VALKYRIE

Fault-tolerant game telemetry pipeline, a data engineering portfolio project.
Flow: simulator -> Kafka -> Spark Structured Streaming -> Delta Lake
(bronze/silver/gold) -> dbt -> PostgreSQL star schema -> dashboard.
Airflow orchestrates batch jobs. Prometheus/Grafana monitor it.
The game and its champions are fictional (no real studio IP).

## Core goals
1. Correctness under failure: no lost events, no double counting, even with
   duplicates, late/out-of-order events, killed jobs, restarted brokers,
   malformed data and schema changes. Prove it with automated chaos tests.
2. Operational maturity: monitoring, SLOs, alerting, replay, data quality
   gates, dead-letter queue.
3. Analytics: win rate by champion and patch (SCD Type 2), anti-cheat flags.
4. Presentation: architecture diagram, README with a "what broke and how I
   fixed it" section, honest benchmarks.

## Tech stack
Python 3.11, Kafka (KRaft, single broker), PySpark Structured Streaming,
Delta Lake, Parquet, PostgreSQL, dbt-postgres, Airflow, Prometheus + Grafana,
Docker Compose with profiles, pytest, GitHub Actions, Makefile.
JSON events in phases 1-4, Avro + Schema Registry in phase 5.
Pin all dependency versions. If unsure of a version or API, say so.

## Hard constraints
- Laptop: WSL2 (Ubuntu), about 10 GB RAM for WSL/Docker, 8 CPU threads.
- Spark in local mode with explicit memory limits, single Kafka broker,
  Compose profiles so not everything runs at once.
- Benchmarks target 1-50M events; be honest about numbers.
- Everything reproducible via `make up`, `make demo`, `make test`.

## Repo layout
simulator/ streaming/ lakehouse/ warehouse/ airflow/ monitoring/
dashboard/ tests/chaos/ docs/ docker/ Makefile README.md

## Engineering standards
- Build in phases. Each phase starts with a short plan, file list and design
  decisions with tradeoffs, and ends with run/verify instructions and tests.
- Production-quality code: type hints, docstrings, config via env/YAML,
  structured logging, error handling, no hardcoded secrets.
- Keep docs/DECISIONS.md (architecture decisions) and docs/WHAT_BROKE.md
  (bugs hit and fixes) updated each phase.
- Do not claim "exactly-once" loosely: it is effectively-once via idempotent
  sinks, and tests must prove it.
- Commit small and often.

## Phases
1 Simulator + Kafka + Spark job writing Parquet
2 Delta, dedupe, watermarks, idempotent MERGE
3 Medallion layers + quality gates + dead-letter queue and replay
4 Chaos/correctness harness (kill Spark, restart Kafka, 5% duplicates,
  shuffled order, late events; assert unique in == rows out)
5 Avro + Schema Registry + schema evolution demo
6 PostgreSQL star schema + dbt (incremental, tests, docs, SCD2)
7 Airflow DAGs
8 Prometheus/Grafana, SLOs, alert demo
9 Dashboard
10 Benchmarks, README, diagrams, CI
