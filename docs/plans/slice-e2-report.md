# Phase 1 slice e2 report: sender, realtime pace, config and flags

Date: 2026-11-15. Commit (local, not pushed): `fd7ece1`.

## Result

The simulator now has two pace modes, with backfill as the default, and optional deliberate
lateness. The D1.7c bug is gone: in every real run, zero events were sent before their own
`event_time` (it was +0.2 s to +124.5 s ahead before).

On my side: `make test-unit` 221 passed, `make lint` clean, `make smoke` PASS,
`make smoke-restart` PASS (8/8 checks), `make demo` PASS, `make verify` PASS for three runs.

Nothing contradicted the plan. No design change was needed.

## Your question: what decides slight versus very?

A stated config value: `late_too_late_share`, default **0.2**. A late event is `very` when its
second hash number `u2` is below it. YAML key `late_too_late_share`, env
`SIM_LATE_TOO_LATE_SHARE`, flag `--late-too-late-share`. The 22% you saw (78 of 356) is
sampling noise around 20%.

## What was built

| File | Change |
|---|---|
| `simulator/sender.py` (new) | `SendStats`, `send_backfill`, `send_realtime`; all clocks injectable |
| `simulator/config.py` | `pace`, `time_scale`, `late_fraction`, `late_too_late_share`, `assumed_watermark_s`; validation; `lateness()` |
| `simulator/main.py` | `prepare_run(cfg, *, now, dry_run=False)`; five new flags; `_send_all` replaced by the sender |
| `simulator/producer.py` | `poll(timeout_s)`: waits while serving delivery callbacks |
| `simulator/config.yaml` | documents the new settings |
| `tests/unit/test_sender.py` (new), `tests/unit/test_prepare_run.py` (new), `tests/unit/test_config.py` | 37 new test cases |

New settings and defaults (the defaults keep both features off):

| Setting | Flag | Default |
|---|---|---|
| `pace` | `--pace` | `backfill` |
| `time_scale` | `--time-scale` | `1.0` |
| `late_fraction` | `--late-fraction` | `0.0` |
| `late_too_late_share` | `--late-too-late-share` | `0.2` |
| `assumed_watermark_s` | `--assumed-watermark-s` | `30` |

Rejected combinations (exit code 2, confirmed with real commands):
- backfill with a `--start-time` that puts the last event in the future
- realtime with `--start-time` (allowed with `--dry-run`)
- realtime with `--rate`

## Condition 9: confirmed with real runs

### Backfill run, event times relative to the wall clock (`make sim-tiny SEED=701`)

| | Value |
|---|---|
| Wall clock when the schedule was built | 10:12:33.146 |
| First `event_time` | 10:10:42.345, which is 110.801 s in the past |
| Last `event_time` | 10:12:33.146, which is 0.000 s in the past |
| Events sent before their `event_time` | 0 |
| `max_ahead_ms` | -2 (the newest event was already 2 ms old when sent) |

### Predictions from e1 against reality

| Command | Predicted | Actual |
|---|---|---|
| `make sim-tiny` | about 2.3 min ago to now | 110.8 s ago to now, 14/14 delivered |
| `make smoke` | same, still PASS | PASS, 14/14 ids, 0 duplicates; manifest shows `pace: backfill`, timeline start 99 s before the run |
| `make demo` | about 50 min ago to now, still PASS | PASS, 1261/1261 ids, 0 duplicates; timeline start 45 min before the run |
| `make smoke-restart` | PASS | PASS, all 8 checks, 12/12 ids |
| `make verify` | PASS | PASS for seeds 701, 702, 703 (backfill, realtime, realtime with late events) |

(a) Existing commands with an explicit `--start-time`: there are none (Makefile, compose file
and e2e scripts checked), so no existing command fails the new check.

(b) `make verify` and `make smoke-restart` pass, as predicted: they use event_id and offsets.

### Realtime runs

| Run | Duration | Late events | `future_events` | `max_behind_ms` |
|---|---|---|---|---|
| seed 702, `--pace realtime --time-scale 10` | 9.7 s | 0 | 0 | 0 |
| seed 703, same plus `--late-fraction 0.3` | 79 s | 5 slight, 1 very | 0 | 0 |

### Independent check against Kafka's own timestamps

I read the three runs back from Kafka with a throwaway script and compared each message's
`CreateTime` (stamped by the Kafka client) with its `event_time`. This becomes
`make timing-check` in e4.

| Run | Events ahead of `CreateTime` | `CreateTime - event_time` | Out-of-order pairs in a partition |
|---|---|---|---|
| 701 backfill | 0 | 2 ms to 110,802 ms | 0 |
| 702 realtime | 0 | 0 ms for every event | 0 |
| 703 realtime, late | 0 | 0 ms to 70,280 ms | 4 |

So the 1 ms tolerance for "ahead: 0" held on this WSL2 machine, with room to spare.

## The unsure points, resolved

1. `Producer.poll` returning early: harmless; the loop re-checks the clock. Real runs were on
   time to the millisecond.
2. 1 ms tolerance on WSL2: held (table above).
3. Realtime starts 1 s after "now": works; first events were not overdue (`max_behind_ms` 0).
4. Realtime with `--start-time`: rejected in `prepare_run`, allowed with `--dry-run`.
5. The v2 manifest's `config` section now has the five new keys. The reader ignores them and
   verify still passes. The proper `pacing` section is e3.

## Other visible changes (none affect ids, counts, offsets or exit codes)

- The Kafka check now runs before generation, so the log shows `kafka ready` before
  `generated events`. A Kafka outage still gives exit code 2.
- The `generated events` and `run complete` log lines have extra fields (pace, event-time
  ages, late counts, `future_events`, `max_ahead_ms`, `max_behind_ms`).
- The manifest's `start_time` now has millisecond precision.
- `--dry-run` prints events in planned send order (identical to before when lateness is off).
- If `future_events` is ever above 0, the simulator logs a WARNING. The exit code is unchanged.

One thing went wrong while building: a unit test hung because my fake clock could not reach a
due time. It was a bug in the test helper, not in the sender, and is fixed.

Known limitation, unchanged from before: all events are held in memory. Very large realtime
runs will also start slightly behind, because the schedule is built after "now" is taken;
`max_behind_ms` shows it.

## How to verify

Seeds 701-703 are already in Kafka from my runs, so use the fresh seeds below. Re-using a seed
creates duplicates on purpose (D1.6).

```bash
make test-unit
```
All unit tests, no Docker. Expect `221 passed`.

```bash
make lint
```
The ruff linter. Expect `All checks passed!`.

```bash
make sim-tiny SEED=711
```
A backfill run (the default pace). In the `generated events` line expect `"pace": "backfill"`,
`"events": 14`, `"first_event_age_s"` near 91 and `"last_event_age_s": 0.0`. In `run complete`
expect `"delivered": 14`, `"future_events": 0` and `"max_ahead_ms"` zero or negative.

```bash
.venv/bin/python -m simulator.main --config simulator/config.tiny.yaml --seed 712 --pace realtime --time-scale 10
```
A realtime run, game time ten times faster. Takes about 13 s. Expect `"events": 13`,
`"future_events": 0`, `"max_ahead_ms": 0` and `"max_behind_ms"` of a few ms at most.

```bash
.venv/bin/python -m simulator.main --config simulator/config.tiny.yaml --seed 713 --pace realtime --time-scale 10 --late-fraction 0.3
```
Realtime with 30% late events. Takes about 85 s, because one very late event is sent 60-90 s
after it "happened". Expect `"events": 10`, `"late_slight": 1`, `"late_very": 1`,
`"future_events": 0`.

```bash
make verify MANIFEST=$(ls manifests/run_*_seed713.json)
```
Compares raw Parquet with the manifest of the late run (Spark must be running; `make smoke`
starts it). Expect `"result": "PASS"` with 10 ids and nothing missing: late events are still
in the raw layer.

```bash
.venv/bin/python -m simulator.main --config simulator/config.tiny.yaml --seed 714 --start-time 2026-11-15T23:59:00Z; echo "exit=$?"
```
A start time whose timeline ends in the future. Expect an `invalid configuration` error that
says "in the future" and `exit=2`. Nothing is sent. (After 2026-11-16 this date is in the past
and the run would be sent; use any future date then.)

```bash
make smoke
make smoke-restart
```
The two existing end-to-end checks. Expect `"result":"PASS","exit_code":0` in each SUMMARY line.

## What to paste back

1. The last line of `make test-unit` and of `make lint`.
2. The `generated events` and `run complete` lines of the seed 711 run.
3. The `run complete` line of the seed 712 run and of the seed 713 run.
4. The `"result"` line of `make verify`.
5. The error line and `exit=` line of the seed 714 command.
6. The SUMMARY lines of `make smoke` and `make smoke-restart`.

## Interview questions

1. Realtime pacing tracks due times on a monotonic clock but checks the wall clock before each
   send. Why are both needed?
2. In backfill mode nothing waits, so what does a "late" event mean there, and why can backfill
   not demonstrate watermark drops?
3. Why does the realtime wait use `producer.poll(timeout)` instead of `time.sleep(timeout)`?
