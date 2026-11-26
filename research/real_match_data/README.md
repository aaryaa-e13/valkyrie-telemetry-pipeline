# Real match data exploration

> VALKYRIE isn't endorsed by Riot Games and doesn't reflect the views or opinions of Riot
> Games or anyone officially involved in producing or managing Riot Games properties. Riot
> Games, and all associated properties are trademarks or registered trademarks of Riot
> Games, Inc.

This folder is a one-off research step, **not part of the pipeline**. VALKYRIE's game,
champions and simulator stay fictional. Here, about 50 real ranked matches are fetched from
the Riot Games API (Match-V5: match + timeline) so that their fields, nulls, odd values and
timestamp encoding can be compared with the simulator's events. The results feed the Phase 3
quality-gate design.

**The sample:** recent ranked solo games of top-ladder players on one server (the challenger
ladder of platform `sg2`), so the same players appear in many matches. Small n. It is not
representative of all games.

No Kafka, Spark or Delta is used. Standard library Python only.

## What is committed, and what is not

| Committed (this folder) | Never committed |
|---|---|
| `fetch_matches.py`, `compare_schema.py`, their tests in `tests/unit/` | Raw API responses (`realdata/`, gitignored) |
| [`FINDINGS.md`](FINDINGS.md) and `findings.json`: counts and field paths only | Match ids, player ids (PUUIDs), names, tags |
| | Raw event payloads, the API key |

## Fetching

You need a development API key from the Riot developer portal. Dev keys deactivate every
24 hours. The key is read from the `RIOT_API_KEY` environment variable only and sent only as
the `X-Riot-Token` header over HTTPS; it is never printed, logged, written or put in a URL.
Every call sends an explicit, honest User-Agent (`valkyrie-research/0.1.0 (...)`): the
network edge in front of the API blocks urllib's default one with 403 "error code: 1010".
A 401/403 counts as "key rejected" only if its body is the API's JSON status; any other
401/403 stops as "blocked by the network edge" (exit 6).

```bash
read -rsp "Riot API key: " RIOT_API_KEY && export RIOT_API_KEY; echo
.venv/bin/python -I research/real_match_data/fetch_matches.py --platform sg2 --region sea
unset RIOT_API_KEY
```

- Call 1 reads the challenger ladder (`--platform` host). Then, in the order returned, one
  page of each player's newest 20 ranked solo ids (queue 420, `--region` host), at most 5
  players, and match + timeline per id until 50 pairs. A snowball over match participants
  (at most 3 pages) is used only if that is not enough.
- Budget: at most 110 calls (104-106 expected), one every 1.25 s, so about 2.5-3 minutes.
- Exit 5 if no ladder entry has a `puuid` field; exit 4 if the players used have no ranked
  solo ids. Neither switches to another seed method.
- A re-run only fetches files that are missing.
- The last line is `SUMMARY {json}` with counts only. Exit codes are in the script's
  docstring.

## Comparing with the simulator

```bash
.venv/bin/python -m simulator.main --config simulator/config.yaml --seed 42 \
  --matches 50 --dry-run > realdata/sim_events.jsonl
.venv/bin/python -I research/real_match_data/compare_schema.py \
  --raw realdata/raw --sim realdata/sim_events.jsonl
```

The first command is the simulator's existing dry run: no Kafka, no manifest. The second
writes `findings.json` and `FINDINGS.md` here, but only if the totals reconcile and no
identifier read from the raw files appears in either file (exit 1 otherwise, nothing
written). See [`FINDINGS.md`](FINDINGS.md).

## API observations from the live fetch (2026-11-26)

- 104 calls (ladder 1, match-id pages 3, matches 50, timelines 50), all HTTP 200, 0 retries,
  131.1 s. The ladder had 300 entries, all with a `puuid`; 3 ladder players gave 60 ids.
- App rate limit of the dev key: `x-app-rate-limit: 100:120,20:1`. Method limits differ per
  endpoint: ladder `500:600,30:10`, match `2000:10`.
- Observation only, interpretation UNVERIFIED: the last app count read `7:120` after about
  95 calls in 120 s, which suggests fixed windows rather than a sliding window. Pacing was
  not changed.
- The first live runs got HTTP 403 `error code: 1010` from the network edge, because of
  urllib's default User-Agent (see `docs/WHAT_BROKE.md`).
