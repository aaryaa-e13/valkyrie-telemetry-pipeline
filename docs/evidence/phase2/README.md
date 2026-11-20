# Phase 2 evidence

The run outputs that the Phase 2 numbers in `README.md` and `docs/DECISIONS.md` (D2.4-D2.6)
come from, so a reader can check them without the working logs (which are not in the repo).

**How these files were made (2026-11-20).** Each file is a copy of what a run printed: an e2e
`SUMMARY {json}` line, a `make timing-check` JSON line, a probe's JSON output, or the last
lines of a log. One change was applied: a JSON string starting with `/data/` (the repo's
`./data` folder, which the containers mount at `/data`) is written as `data/...` (14 strings,
all in the slice 6 guard records). Nothing else was edited. Before committing, every file was
scanned for absolute paths, user names, e-mail addresses, secrets and raw event payloads:
0 hits. The SUMMARY lines hold counts, verdicts, run ids and manifest file names, not event
contents.

**Not here:** runs from slices 1-3 and most of slice 4 (their numbers are only in the slice
reports, which stayed outside the repo), and the final Phase 2 gate (its result is in the
`phase-2` tag message).

## slice4/ (D2.4 "Timing follow-up")

| File | What it is |
|---|---|
| `before_silver_steps.json` | `bench/silver_steps.py` over silver batches 22-24: median wall time per step (`median_wall_ms`), before the one-aggregation trial |
| `after_silver_steps.json` | the same over batches 25-27, with the trial (reverted afterwards) |
| `wm_probe_50.json` | `bench/wm_probe.py`, 50-row re-check: watermark read time on the live control table (22 rows) and on a test control table grown from 1 to 50 rows. D2.4's 500-row figures come from an earlier probe run whose raw output was not kept; they are in the slice 4 timing report only |

## slice5/ (D2.5, `make smoke-late` and the regression runs)

| File | What it is |
|---|---|
| `smoke-late-run{1,2,3}.summary.json` | the three smoke-late runs: `events`, `oracle` (verdicts R4-R6d, both gates, `late_counts`, timing), `memory` (container and JVM peaks), `total_s` |
| `smoke-late-run{1,2,3}.timing-check.json` | `make timing-check` on each run's manifest: `ahead` = events sent before their `event_time` (0 in all three) |
| `smoke.summary.json`, `smoke-dupes.summary.json`, `smoke-restart.summary.json` | the regression runs after the smoke-late runs (`result`, `total_s`, verify summary) |

## slice6/ (D2.6, `make smoke-faults`)

| File | What it is |
|---|---|
| `after_read-run2.summary.json`, `after_late.summary.json`, `after_silver.summary.json`, `after_control.summary.json` | one silver fault point each: `marker`, `killed_container` (exit code, OOM), `lake_before` / `lake_down` / `lake_after` (rows and orphan files per table), `guards` (T, C, P at the restart), `control_row_n`, `verdicts`, `memory`, `seconds` |
| `inside_bronze.summary.json` | the bronze attempt: batch, `offsets`/`commits` files, bronze txn, `state` ("i"), guard, rows added |
| `after_read-run1.marker.json`, `after_read-run1.posthoc.txt` | the first `after_read` run, which fired but lost its down-state evidence (WHAT_BROKE): its marker, and the read-only checks made after the restart. Counted as "fired, evidence incomplete", not as a pass |
| `k5-checks.txt` | after all fault runs: `VALKYRIE_FAULT*` entries in the running container (0) and `make faults-config-check` (0) |
| `k5-smoke.summary.json` | the normal `make smoke` run after the fault runs |
| `test-spark-k6.txt` | the last line of the full Spark suite run with the hooks in place and inert (111 passed) |
