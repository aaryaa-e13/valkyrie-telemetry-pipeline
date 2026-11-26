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

```bash
read -rsp "Riot API key: " RIOT_API_KEY && export RIOT_API_KEY; echo
read -rp "Riot ID (name#tag): " RIOT_ID && export RIOT_ID
.venv/bin/python -I research/real_match_data/fetch_matches.py --region europe
unset RIOT_API_KEY RIOT_ID
```

- `--region`: `americas`, `asia`, `europe` or `sea`, the regional host of your server.
- Budget: at most 110 calls (about 104 expected), one every 1.25 s, so about 2.5-3 minutes.
- Ranked solo queue only (queue 420). If the account has no ranked solo matches, the
  script stops (exit 4).
- A re-run only fetches files that are missing.
- The last line is `SUMMARY {json}` with counts only. Exit codes are in the script's
  docstring.
