# bench/: measurement probes

Read-only probes behind the numbers in `docs/DECISIONS.md` D2.4. Phase 10 reuses them.
Not part of `make test`.

- `silver_steps.py`: per-step time of silver batches (wm read, plan counts, late write,
  late anti-join, silver MERGE, control write) from the live job's Spark REST API and logs.
  `python3 bench/silver_steps.py --list`, then
  `python3 bench/silver_steps.py 19 20 21 --out <file>.json` (also writes `<file>.raw.json`).
- `wm_probe.py`: wm read and control write cost vs control-table size, in a throwaway table
  in the `spark-tools` container's `/tmp` (`./data` read only).
  `SPARK_TOOLS_MEM_LIMIT=3g docker compose --profile tools run --rm -T -v ./bench:/probe:ro
  spark-tools python /probe/wm_probe.py 500 > <file>.json`
