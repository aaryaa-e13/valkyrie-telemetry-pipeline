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
| `fetch_matches.py`, its tests in `tests/unit/` | Raw API responses (`realdata/`, gitignored) |
| Findings as counts and field paths only (later step) | Match ids, player ids (PUUIDs), names, tags |
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
