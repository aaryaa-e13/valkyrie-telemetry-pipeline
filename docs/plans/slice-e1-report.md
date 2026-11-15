# Phase 1 slice e1 report: pure send planning

Date: 2026-11-15. Commits (local, not pushed): `7f1ea2d` (event_id pins), `8afa255` (e1).

## What was built

- `tests/unit/test_event_id_pins.py`: written and committed BEFORE any other change. Pins the
  count, the first five event_ids and the checksum of all ids for seeds 42 and 7.
- `simulator/schedule.py` (new, pure: no Kafka, no clock, no sleeping).
- `tests/unit/test_schedule.py` (new, 38 tests).

No existing file was edited. Nothing calls `schedule.py` yet, so the simulator behaves exactly
as before. Wiring it in is slice e2.

## Functions

```python
retime(events, *, ref_start, new_start, time_scale=1.0) -> list[GameEvent]
backfill_start(events, *, ref_start, now, time_scale=1.0) -> datetime
lateness_uniforms(seed, event_id) -> tuple[float, float, float]
plan_delay(seed, event_id, lateness) -> tuple[int, str | None]      # (delay_ms, band)
build_schedule(events, seed, lateness) -> list[PlannedSend]
```

- `retime` moves and optionally compresses the timeline. Only `event_time` changes;
  `duration_s` is not scaled.
- `backfill_start` returns the start time that puts the last event at or just before `now`.
- `build_schedule` returns every event once, sorted by `(send_at, event_time, match_id, match_seq)`.

## Lateness formula (goes into DECISIONS.md in e4)

```
digest = hashlib.sha256(f"{seed}:lateness:{event_id}".encode("utf-8")).digest()
u1, u2, u3 = first three 8-byte big-endian integers of digest, each divided by 2**64
late   = u1 < late_fraction
band   = "very" if u2 < too_late_share else "slight"
lo, hi = (2.0, 3.0) if band == "very" else (0.1, 0.5)
delay_ms = round((lo + u3 * (hi - lo)) * assumed_watermark_s * 1000)
```

Python's `hash()` is not used anywhere. A test runs the formula in two fresh processes with
different `PYTHONHASHSEED` values and checks the result is identical.

## One refinement to flag (condition 8)

`expected` is decided by the band, not by `stream_lag_ms`:

- on time or `slight` -> `keep`
- `very` -> `may_drop`

Reason: sending to Kafka is asynchronous. A very late event with a small lag is only safe if
nothing newer overtakes it on the way to the broker, and that cannot be promised. A `slight`
event is at most 0.5 x watermark behind, so it is safe even if a neighbour overtakes it.
`stream_lag_ms` is still recorded as information. This does not change the approved design; it
fixes a rule the plan had left open.

## Answers to conditions 2, 3, 4 (to be built in e2-e4)

| Condition | Answer |
|---|---|
| 2. `make sim-realtime` | tiny config, `time_scale` 10. Timeline is at most 138 s of game time, so about 14 s, plus flush. Expect under 30 s. |
| 2. `make sim-late` | same, plus 30% late events. The longest delay is 3 x 30 s = 90 s. Expect about 105-110 s, at most 2 minutes. |
| 3. "ahead: 0" tolerance | 1 ms. `event_time` is whole milliseconds and Kafka's `CreateTime` is whole milliseconds, so 1 ms covers rounding. The sender will check the wall clock before each send. If e2 shows 1 ms is flaky on WSL2, I stop and tell you. |
| 4. `late_events` list omitted when | (a) `--no-manifest-ids` is set, or (b) there are more than 100,000 late events (about 20 MB of JSON). Per-band counts are always written, and the manifest says why the list is missing. |

## Prediction: what backfill-by-default changes (condition 9)

Only `event_time` values and the manifest's `start_time` field change. Event ids, counts,
partitions, offsets and send order stay the same.

| Command | Today | After e2 (predicted) |
|---|---|---|
| `make sim`, `make sim-tiny`, `make smoke` (tiny config) | `event_time` from now to about +2.3 min | from about 2.3 min ago to now |
| `make sim-local` (default config, 2 matches) | now to about +41 min | about 41 min ago to now |
| `make demo` (default config, 20 matches) | now to about +50 min | about 50 min ago to now |
| `make sim-dry` | starts at now | ends at now |
| `make verify`, `make smoke-restart` | pass | pass: they use event_id and Kafka offsets; no e2e or verify code reads `event_time` (checked with grep) |

- No Makefile target, compose file or e2e script passes `--start-time` today (checked with
  grep), so I predict no existing command will fail the new "last event in the future" check.
- Raw Parquet is partitioned by `ingest_date`, not by event time, so file layout does not change.
- Side effect to know: with the default config, event times are up to 50 minutes old on arrival.
  That is correct for backfill, and it is why Phase 2 late-drop tests must use realtime pace.

All of this gets confirmed with real runs in the e2 report.

## Results on my side

- `make test-unit`: 184 passed (144 before, +2 pins, +38 schedule).
- `ruff check .`: all checks passed.
- `make smoke`: PASS, 12/12 ids, 0 duplicates, exit code 0.

## How to verify

```bash
make test-unit
```
Runs all unit tests on the laptop (no Docker). Expect `184 passed`.

```bash
.venv/bin/python -m pytest tests/unit/test_schedule.py tests/unit/test_event_id_pins.py -q
```
Runs only this slice's tests. Expect `40 passed`.

```bash
make lint
```
Runs the ruff linter. Expect `All checks passed!`.

```bash
git diff --stat 5175f95 HEAD
```
Lists files changed since before the slice. Expect exactly three files, all new:
`simulator/schedule.py`, `tests/unit/test_event_id_pins.py`, `tests/unit/test_schedule.py`.

```bash
.venv/bin/python -c "
from datetime import UTC, datetime
from collections import Counter
from simulator.config import SimulatorConfig
from simulator.generator import generate_events
from simulator.schedule import LatenessConfig, build_schedule
events = generate_events(SimulatorConfig(seed=42, num_matches=20), datetime(2026, 11, 14, 12, tzinfo=UTC))
plan = build_schedule(events, 42, LatenessConfig(late_fraction=0.3))
print('events', len(plan), 'bands', sorted(Counter(str(p.band) for p in plan).items()))
print('expected', sorted(Counter(p.expected for p in plan).items()), 'max_lag_ms', max(p.stream_lag_ms for p in plan))
"
```
Builds a schedule for 20 matches with 30% late events and prints a summary. Expect exactly:
```
events 1242 bands [('None', 886), ('slight', 278), ('very', 78)]
expected [('keep', 1164), ('may_drop', 78)] max_lag_ms 89003
```

```bash
make smoke
```
The existing end-to-end check (Kafka and Spark in Docker, about 30-60 s). Expect `PASS` and
`"exit_code":0` in the SUMMARY line.

## What to paste back

1. The last line of `make test-unit`.
2. The last line of the 40-test pytest command.
3. The output of `git diff --stat 5175f95 HEAD`.
4. The two lines printed by the schedule summary command.
5. The `SUMMARY` line of `make smoke`.

## Interview questions

1. Why is the lateness of an event derived from a sha256 hash of `seed` and `event_id`, and what
   would go wrong with Python's built-in `hash()` or with the generator's own RNG?
2. A late event has `delay_ms = 70000` but `stream_lag_ms = 0`. How is that possible, and which
   of the two numbers does a watermark care about?
3. Why does `retime` leave `duration_s` alone when it compresses the timeline with `time_scale`?
