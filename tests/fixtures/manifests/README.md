# Manifest fixtures

Real manifests written by the simulator, copied unchanged from `manifests/`
(which is git-ignored). They let a fresh clone or CI prove that the reader
still accepts every manifest version ever written. Never edit them by hand.

| File | Version | Written by | Events | Notes |
|---|---|---|---|---|
| `v1_run_20261114T131326Z_seed42.json` | 1 | Phase 1b | 14 | no `kafka_offsets` |
| `v2_pre_e2_run_20261115T095317Z_seed1794736395320.json` | 2 | Phase 1d (a `make smoke` run) | 12 | before pacing existed |
| `v2_e2_run_20261115T101248Z_seed703.json` | 2 | Phase 1e2 | 13 | `config` has the five pacing/lateness keys; realtime run with late events |
| `v3_run_20261115T105409Z_seed723.json` | 3 | Phase 1e3 | 13 | realtime, 5 slightly late + 3 very late events |

Tests: `tests/unit/test_manifest_fixtures.py`, `tests/spark/test_verify_manifest.py`.
