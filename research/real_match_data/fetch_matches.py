"""Fetch ranked solo matches and their timelines from the Riot Games API (Match-V5).

Real-data exploration for VALKYRIE. Not part of the pipeline: no Kafka, no Spark, no Delta.
Standard library only and self-contained, so it runs with ``python -I`` (isolated mode).

Seed: the challenger ladder of one platform (League-V4, RANKED_SOLO_5x5). Players are taken
in the order returned, one page of their newest 20 ranked solo ids (queue 420) each, at most
5 players, then a snowball over match participants (at most 3 pages) if still short.

Raw responses are written unchanged to a gitignored folder (default ``realdata/raw``):
``ladder.json``, ``ids.json``, ``matches/<id>.json``, ``timelines/<id>.json`` and
``fetch_log.json`` (counts only: no key, no ids, no names, no ladder positions).

API key rules:
- read only from the ``RIOT_API_KEY`` environment variable;
- sent only as the ``X-Riot-Token`` header, over HTTPS, with redirects refused (a redirect
  would forward the header to another host);
- never printed, logged, written to a file or put in a URL. Every log line passes through a
  mask that replaces the key with ``***``.

Rate limits: one call every 1.25 s (under 20/1 s, 100/2 min and 10/10 s), at most 110 calls
per run including retries. HTTP 429 waits ``Retry-After`` (stops if it is missing or over
120 s). 401/403 stop at once. Three failures in a row stop the run. Files already on disk are
not fetched again, so a re-run after a stop only makes the missing calls.

Usage (from the repo root):
    read -rsp "Riot API key: " RIOT_API_KEY && export RIOT_API_KEY; echo
    .venv/bin/python -I research/real_match_data/fetch_matches.py --platform sg2 --region sea

Exit codes: 0 done, 1 stopped early (failures, rate limit, call cap, short), 2 setup problem,
3 key rejected (HTTP 401/403), 4 0 ranked solo ids from the ladder players used,
5 no ladder entry has a puuid field, 130 interrupted.
"""

from __future__ import annotations

import argparse
import http.client
import json
import logging
import math
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, TextIO

# --- API facts (UNVERIFIED in the docs pages read on 2026-11-26; confirmed by the probe) ---
KEY_ENV = "RIOT_API_KEY"
TOKEN_HEADER = "X-Riot-Token"
HOST_TEMPLATE = "https://{region}.api.riotgames.com"
REGIONS: tuple[str, ...] = ("americas", "asia", "europe", "sea")
# Platform hosts. Only na1, euw1, kr and the regional four are in the docs page read; the
# rest (sg2 included) are from memory: UNVERIFIED. An allowlist, so no other host is built.
PLATFORMS: tuple[str, ...] = (
    "br1", "eun1", "euw1", "jp1", "kr", "la1", "la2", "me1", "na1", "oc1", "ru", "sg2",
    "tr1", "tw2", "vn2",
)
LADDER_PATH = "/lol/league/v4/challengerleagues/by-queue/{queue}"
LADDER_QUEUE = "RANKED_SOLO_5x5"
IDS_PATH = "/lol/match/v5/matches/by-puuid/{puuid}/ids"
MATCH_PATH = "/lol/match/v5/matches/{match_id}"
TIMELINE_PATH = "/lol/match/v5/matches/{match_id}/timeline"
QUEUE_RANKED_SOLO = 420
RATE_LIMIT_HEADERS: tuple[str, ...] = ("x-app-rate-limit", "x-method-rate-limit")
RATE_HEADERS: tuple[str, ...] = (
    "x-app-rate-limit", "x-app-rate-limit-count",
    "x-method-rate-limit", "x-method-rate-limit-count",
)

# --- Budget and pacing ---
PAGE_SIZE = 20
MAX_LADDER_PLAYERS = 5
MAX_SNOWBALL_PAGES = 3
DEFAULT_MATCHES = 50
MIN_INTERVAL_S = 1.25
MAX_CALLS = 110
MAX_RETRY_AFTER_S = 120.0
MAX_FAILURES_IN_ROW = 3
SERVER_ERROR_WAIT_S = 2.0
HTTP_TIMEOUT_S = 15.0
PROGRESS_EVERY = 10

# Match ids come from the API (untrusted) and become file names: allow only this shape.
MATCH_ID_RE = re.compile(r"^[A-Z0-9]{2,5}_[0-9]{1,20}$")

EXIT_OK = 0
EXIT_STOPPED = 1
EXIT_SETUP = 2
EXIT_KEY_REJECTED = 3
EXIT_NO_RANKED_IDS = 4
EXIT_NO_PUUID = 5
EXIT_INTERRUPTED = 130

REPO_ROOT = Path(__file__).resolve().parents[2]
ALLOWED_OUT_IN_REPO = REPO_ROOT / "realdata"

NO_RANKED_IDS_MSG = (
    f"0 ranked solo match ids (queue {QUEUE_RANKED_SOLO}) from the ladder players used; "
    "stopping "
    "(no other seed method without approval)"
)
KEY_REJECTED_MSG = (
    "key rejected (HTTP {status}): check RIOT_API_KEY; dev keys deactivate every 24 h"
)


# --- Transport ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class HttpResponse:
    """One HTTP response. Header names are lower-case."""

    status: int
    headers: Mapping[str, str]
    body: bytes


class TransportError(Exception):
    """A network-level failure with no HTTP status (DNS, TLS, timeout, reset)."""


Transport = Callable[[urllib.request.Request, float], HttpResponse]


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """Refuse redirects: urllib would copy the key header to the redirect target."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[override]
        return None


_OPENER = urllib.request.build_opener(_NoRedirect())


def _lower_headers(headers: Any) -> dict[str, str]:
    return {str(k).lower(): str(v) for k, v in (headers.items() if headers else [])}


def urllib_transport(request: urllib.request.Request, timeout: float) -> HttpResponse:
    """Send one request with urllib. HTTP errors become responses; network errors raise.

    Raises:
        TransportError: no HTTP response was received. The message holds only the error
            type and reason, never the request headers.
    """
    try:
        with _OPENER.open(request, timeout=timeout) as resp:
            return HttpResponse(resp.status, _lower_headers(resp.headers), resp.read())
    except urllib.error.HTTPError as exc:  # before URLError: HTTPError is a subclass
        body = exc.read() if exc.fp is not None else b""
        return HttpResponse(exc.code, _lower_headers(exc.headers), body)
    except (urllib.error.URLError, http.client.HTTPException, TimeoutError, OSError) as exc:
        reason = getattr(exc, "reason", exc)
        raise TransportError(f"{type(exc).__name__}: {reason}") from None


# --- Masking and logging -----------------------------------------------------------------


def mask(text: str, secret: str) -> str:
    """Return ``text`` with every occurrence of ``secret`` replaced by ``***``."""
    if not secret:
        return text
    return text.replace(secret, "***")


class _JsonFormatter(logging.Formatter):
    """One JSON object per line; the finished line is masked as a last step."""

    def __init__(self, secret: str) -> None:
        super().__init__()
        self._secret = secret

    def format(self, record: logging.LogRecord) -> str:
        created = datetime.fromtimestamp(record.created, UTC)
        out: dict[str, Any] = {
            "ts": created.isoformat(timespec="seconds").replace("+00:00", "Z"),
            "level": record.levelname,
            "msg": record.getMessage(),
        }
        out.update(getattr(record, "fields", {}))
        return mask(json.dumps(out, default=str), self._secret)


def make_logger(stream: TextIO, secret: str) -> logging.Logger:
    """Return this script's logger, writing masked JSON lines to ``stream`` only."""
    logger = logging.getLogger("realdata.fetch")
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
    handler = logging.StreamHandler(stream)
    handler.setFormatter(_JsonFormatter(secret))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    return logger


def _log(logger: logging.Logger, level: int, msg: str, **fields: Any) -> None:
    logger.log(level, msg, extra={"fields": fields})


# --- Small parsers -----------------------------------------------------------------------


def parse_retry_after(value: str | None) -> float | None:
    """Return ``Retry-After`` in seconds, or None if missing or not a number of seconds."""
    if value is None:
        return None
    try:
        seconds = float(value.strip())
    except ValueError:
        return None
    if not math.isfinite(seconds) or seconds < 0:
        return None
    return seconds


def parse_rate_limit(value: str) -> list[tuple[int, float]] | None:
    """Parse a rate-limit header such as ``20:1,100:120`` into (limit, window_s) pairs.

    The format is UNVERIFIED (from memory). Returns None if any part does not parse.
    """
    pairs: list[tuple[int, float]] = []
    for part in value.split(","):
        limit_s, sep, window_s = part.strip().partition(":")
        if not sep:
            return None
        try:
            limit, window = int(limit_s), float(window_s)
        except ValueError:
            return None
        if limit <= 0 or window <= 0:
            return None
        pairs.append((limit, window))
    return pairs or None


def max_calls_in_window(window_s: float, interval_s: float) -> int:
    """Most calls that fixed spacing of ``interval_s`` can put into any ``window_s`` window."""
    return math.floor(window_s / interval_s) + 1


def build_url(host: str, path: str, query: Mapping[str, Any] | None = None) -> str:
    """Build an HTTPS API URL for a region or platform host from the allowlists.

    Path parts must already be quoted by the caller.
    """
    if host not in REGIONS and host not in PLATFORMS:
        raise ValueError(f"unknown host {host!r}")
    url = HOST_TEMPLATE.format(region=host) + path
    if query:
        url += "?" + urllib.parse.urlencode(query)
    if not url.startswith("https://"):
        raise ValueError("only https URLs are allowed")
    return url


def _quote(part: str) -> str:
    return urllib.parse.quote(part, safe="")


# --- API client --------------------------------------------------------------------------


class StopFetch(Exception):
    """Stop the whole run. ``reason`` is a short code for the summary."""

    def __init__(self, reason: str, exit_code: int, message: str) -> None:
        super().__init__(message)
        self.reason = reason
        self.exit_code = exit_code
        self.message = message


class ApiClient:
    """Paced, counted GET calls with the failure policy of the plan (section 2)."""

    def __init__(
        self,
        key: str,
        *,
        transport: Transport,
        clock: Callable[[], float],
        sleep: Callable[[float], None],
        logger: logging.Logger,
        min_interval_s: float = MIN_INTERVAL_S,
        max_calls: int = MAX_CALLS,
    ) -> None:
        self._key = key
        self._transport = transport
        self._clock = clock
        self._sleep = sleep
        self._log = logger
        self.min_interval_s = min_interval_s
        self.max_calls = max_calls
        self.calls = 0
        self.calls_by_endpoint: Counter[str] = Counter()
        self.status_counts: Counter[str] = Counter()
        self.failures_in_row = 0
        self.retries = 0
        self.retry_after_wait_s = 0.0
        self.rate_headers: dict[str, str] = {}
        self.last_status: int | str | None = None  # HTTP status of the last call, or "network"
        self._pending_stop: StopFetch | None = None
        self._warned_headers: set[str] = set()
        self._last_start: float | None = None

    # -- internals --

    def _pace(self) -> None:
        if self._last_start is not None:
            wait = self._last_start + self.min_interval_s - self._clock()
            if wait > 0:
                self._sleep(wait)
        self._last_start = self._clock()

    def _check_rate_headers(self, headers: Mapping[str, str]) -> None:
        for name in RATE_HEADERS:
            if name in headers:
                self.rate_headers[name] = headers[name]
        for name in RATE_LIMIT_HEADERS:
            if name not in headers:
                continue
            pairs = parse_rate_limit(headers[name])
            if pairs is None:
                if name not in self._warned_headers:
                    self._warned_headers.add(name)
                    _log(self._log, logging.WARNING, "rate-limit header not understood",
                         header=name, value=headers[name])
                continue
            for limit, window in pairs:
                ours = max_calls_in_window(window, self.min_interval_s)
                if ours > limit and self._pending_stop is None:
                    self._pending_stop = StopFetch(
                        "rate_limit_stricter", EXIT_STOPPED,
                        f"live limit {limit} per {window:g} s ({name}) is stricter than this "
                        f"script's pacing ({ours} calls per {window:g} s); stopping")

    def _send(self, url: str, endpoint: str) -> HttpResponse:
        if self._pending_stop is not None:
            raise self._pending_stop
        if self.calls >= self.max_calls:
            raise StopFetch("call_cap", EXIT_STOPPED,
                            f"call cap reached ({self.max_calls} calls); stopping")
        self._pace()
        self.calls += 1
        self.calls_by_endpoint[endpoint] += 1
        request = urllib.request.Request(
            url, headers={TOKEN_HEADER: self._key, "Accept": "application/json"}, method="GET")
        try:
            response = self._transport(request, HTTP_TIMEOUT_S)
        except TransportError as exc:
            self.status_counts["network"] += 1
            self.last_status = "network"
            raise TransportError(mask(str(exc), self._key)) from None
        self.status_counts[str(response.status)] += 1
        self.last_status = response.status
        self._check_rate_headers(response.headers)
        return response

    def _failure(self, what: str) -> None:
        self.failures_in_row += 1
        _log(self._log, logging.WARNING, f"failure: {what}", call=self.calls,
             failures_in_row=self.failures_in_row)
        if self.failures_in_row >= MAX_FAILURES_IN_ROW:
            raise StopFetch("failures_in_row", EXIT_STOPPED,
                            f"{self.failures_in_row} failures in a row (last: {what}); stopping")

    # -- public --

    def get_json(self, url: str, endpoint: str) -> tuple[Any, bytes] | None:
        """GET one URL and parse it as JSON.

        Args:
            url: Full HTTPS URL (never logged: paths contain player ids).
            endpoint: Short name for logs and counts, e.g. ``"match"``.

        Returns:
            (parsed JSON, raw body bytes), or None if this item is skipped (404, other 4xx,
            invalid JSON, or a second server/network error).

        Raises:
            StopFetch: 401/403, 3 failures in a row, a bad ``Retry-After``, the call cap,
                or a live rate limit stricter than the pacing.
        """
        retried_server_error = False
        while True:
            try:
                response = self._send(url, endpoint)
            except TransportError as exc:
                self._failure(f"{endpoint}: network error ({mask(str(exc), self._key)})")
                if retried_server_error:
                    return None
                retried_server_error = True
                self.retries += 1
                self._sleep(SERVER_ERROR_WAIT_S)
                continue
            status = response.status
            if status == 200:
                try:
                    data = json.loads(response.body)
                except ValueError:
                    self._failure(f"{endpoint}: HTTP 200 but the body is not JSON")
                    return None
                self.failures_in_row = 0
                return data, response.body
            if status in (401, 403):
                raise StopFetch("key_rejected", EXIT_KEY_REJECTED,
                                KEY_REJECTED_MSG.format(status=status))
            if status == 429:
                self._failure(f"{endpoint}: HTTP 429")
                wait = parse_retry_after(response.headers.get("retry-after"))
                if wait is None or wait > MAX_RETRY_AFTER_S:
                    raise StopFetch("retry_after", EXIT_STOPPED,
                                    f"HTTP 429 with Retry-After missing or over "
                                    f"{MAX_RETRY_AFTER_S:g} s; stopping")
                _log(self._log, logging.WARNING, "HTTP 429: waiting Retry-After",
                     endpoint=endpoint, wait_s=wait)
                self._sleep(wait)
                self.retry_after_wait_s += wait
                self.retries += 1
                continue
            if status >= 500:
                self._failure(f"{endpoint}: HTTP {status}")
                if retried_server_error:
                    return None
                retried_server_error = True
                self.retries += 1
                self._sleep(SERVER_ERROR_WAIT_S)
                continue
            self._failure(f"{endpoint}: HTTP {status}, skipped")
            return None


# --- Raw storage -------------------------------------------------------------------------


class RawStore:
    """The raw folder. Writes are atomic (temp file + rename)."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.ladder = root / "ladder.json"
        self.ids = root / "ids.json"
        self.log = root / "fetch_log.json"
        self._dirs = {"match": root / "matches", "timeline": root / "timelines"}

    def ensure(self) -> None:
        for d in self._dirs.values():
            d.mkdir(parents=True, exist_ok=True)

    def path(self, kind: str, match_id: str) -> Path:
        if not MATCH_ID_RE.fullmatch(match_id):
            raise ValueError("match id has an unexpected shape")
        return self._dirs[kind] / f"{match_id}.json"

    def has(self, kind: str, match_id: str) -> bool:
        return self.path(kind, match_id).is_file()

    @staticmethod
    def write(path: Path, data: bytes) -> None:
        tmp = path.with_name(path.name + ".tmp")
        tmp.write_bytes(data)
        os.replace(tmp, path)

    @staticmethod
    def read_json(path: Path) -> Any:
        return json.loads(path.read_bytes())


# --- The fetch ---------------------------------------------------------------------------


@dataclass
class Counts:
    """What the summary reports: counts only (no ids, no names, no ladder positions)."""

    target: int
    pairs_complete: int = 0
    ladder_entries: int = 0
    entries_with_puuid: int = 0
    ladder_players_used: int = 0
    seed_ids_found: int = 0
    snowball_pages: int = 0
    snowball_ids_found: int = 0
    matches_skipped: int = 0
    invalid_ids: int = 0
    probe: str = "not run"


def _valid_ids(raw: Any, counts: Counts) -> list[str]:
    if not isinstance(raw, list):
        counts.invalid_ids += 1
        return []
    good = [x for x in raw if isinstance(x, str) and MATCH_ID_RE.fullmatch(x)]
    counts.invalid_ids += len(raw) - len(good)
    return good


def _entry_puuid(entry: Any) -> str | None:
    puuid = entry.get("puuid") if isinstance(entry, dict) else None
    return puuid if isinstance(puuid, str) and puuid else None


class Fetcher:
    """Ladder -> one id page per top player -> match + timeline pairs -> snowball if short."""

    def __init__(self, client: ApiClient, store: RawStore, logger: logging.Logger, *,
                 platform: str, region: str, target: int) -> None:
        self.client = client
        self.store = store
        self.log = logger
        self.platform = platform
        self.region = region
        self.counts = Counts(target=target)
        self.known_ids: list[str] = []
        self._known: set[str] = set()

    def _meta(self) -> dict[str, Any]:
        return {"platform": self.platform, "region": self.region,
                "ladder_queue": LADDER_QUEUE, "queue": QUEUE_RANKED_SOLO}

    # -- resume --

    def load_ids(self) -> None:
        """Load ``ids.json`` from an earlier run; refuse one for another platform/queue."""
        if not self.store.ids.is_file():
            if self.store.ladder.is_file():
                raise StopFetch("seed_mismatch", EXIT_SETUP,
                                "ladder.json without ids.json on disk; use another --out")
            return
        saved = self.store.read_json(self.store.ids)
        if not isinstance(saved, dict) or any(saved.get(k) != v
                                              for k, v in self._meta().items()):
            raise StopFetch("seed_mismatch", EXIT_SETUP,
                            "ids.json on disk is for another platform, region or queue; "
                            "use another --out")
        self._add_ids(_valid_ids(saved.get("seed_ids", []), self.counts))
        self.counts.seed_ids_found = len(self.known_ids)
        self.counts.snowball_ids_found = self._add_ids(
            _valid_ids(saved.get("snowball_ids", []), self.counts))
        self.counts.ladder_players_used = int(saved.get("ladder_players_used", 0))
        self.counts.snowball_pages = int(saved.get("snowball_pages", 0))
        _log(self.log, logging.INFO, "match ids loaded from disk", ids=len(self.known_ids))

    def _save_ids(self) -> None:
        seed = self.known_ids[: self.counts.seed_ids_found]
        snow = self.known_ids[self.counts.seed_ids_found:]
        payload = {**self._meta(), "seed_ids": seed, "snowball_ids": snow,
                   "ladder_players_used": self.counts.ladder_players_used,
                   "snowball_pages": self.counts.snowball_pages}
        self.store.write(self.store.ids, json.dumps(payload, indent=1).encode("utf-8"))

    # -- steps --

    def ladder_puuids(self) -> list[str]:
        """Read the ladder (call #1, the probe) and return its PUUIDs in returned order."""
        if self.store.ladder.is_file():
            data = self.store.read_json(self.store.ladder)
            self.counts.probe = "skipped (ladder.json on disk)"
            _log(self.log, logging.INFO,
                 "probe skipped: ladder.json on disk; the next call checks the key")
        else:
            url = build_url(self.platform, LADDER_PATH.format(queue=LADDER_QUEUE))
            try:
                result = self.client.get_json(url, "ladder")
            except StopFetch as stop:
                self.counts.probe = "rejected" if stop.reason == "key_rejected" else "failed"
                raise
            if result is None and self.client.last_status == 404:
                raise StopFetch("ladder_not_found", EXIT_SETUP,
                                "ladder not found (HTTP 404): check --platform")
            if result is None:
                raise StopFetch("ladder_failed", EXIT_STOPPED,
                                f"ladder call failed (last status: {self.client.last_status})")
            data, body = result
            self.counts.probe = "ok"
            _log(self.log, logging.INFO, "probe ok: key accepted (HTTP 200)",
                 endpoint="ladder", call=self.client.calls,
                 rate_headers=self.client.rate_headers)
            self._save_ids()  # meta first: a ladder.json on disk always has its ids.json
            self.store.write(self.store.ladder, body)
        entries = data.get("entries") if isinstance(data, dict) else None
        entries = entries if isinstance(entries, list) else []
        puuids: list[str] = []
        for entry in entries:
            puuid = _entry_puuid(entry)
            if puuid is not None and puuid not in puuids:
                puuids.append(puuid)
        self.counts.ladder_entries = len(entries)
        self.counts.entries_with_puuid = sum(_entry_puuid(e) is not None for e in entries)
        _log(self.log, logging.INFO, "ladder read", entries=self.counts.ladder_entries,
             entries_with_puuid=self.counts.entries_with_puuid)
        if not puuids:
            raise StopFetch("no_puuid", EXIT_NO_PUUID,
                            f"ladder has {self.counts.ladder_entries} entries and 0 with a "
                            "puuid field; stopping (a fallback needs approval)")
        return puuids

    def _id_page(self, puuid: str, start: int) -> list[str] | None:
        url = build_url(self.region, IDS_PATH.format(puuid=_quote(puuid)),
                        {"queue": QUEUE_RANKED_SOLO, "start": start, "count": PAGE_SIZE})
        result = self.client.get_json(url, "match_ids")
        if result is None:
            return None
        return _valid_ids(result[0], self.counts)

    def _add_ids(self, ids: list[str]) -> int:
        new = [x for x in ids if x not in self._known]
        self._known.update(new)
        self.known_ids.extend(new)
        return len(new)

    def _fetch_one(self, kind: str, match_id: str) -> bool:
        """Fetch one file unless it is on disk. True if it exists afterwards."""
        if self.store.has(kind, match_id):
            return True
        path_t = MATCH_PATH if kind == "match" else TIMELINE_PATH
        url = build_url(self.region, path_t.format(match_id=_quote(match_id)))
        result = self.client.get_json(url, kind)
        if result is None:
            return False
        self.store.write(self.store.path(kind, match_id), result[1])
        return True

    def fetch_pairs(self, ids: list[str]) -> None:
        for match_id in ids:
            if self.counts.pairs_complete >= self.counts.target:
                return
            if not self._fetch_one("match", match_id):
                self.counts.matches_skipped += 1
                continue
            if not self._fetch_one("timeline", match_id):
                self.counts.matches_skipped += 1
                continue
            self.counts.pairs_complete += 1
            if self.counts.pairs_complete % PROGRESS_EVERY == 0:
                _log(self.log, logging.INFO, "progress",
                     pairs=f"{self.counts.pairs_complete}/{self.counts.target}",
                     calls=self.client.calls)

    def ladder_players(self, puuids: list[str]) -> None:
        """One id page (newest 20) per ladder player, in order, until the target is met."""
        for puuid in puuids[self.counts.ladder_players_used:MAX_LADDER_PLAYERS]:
            if self.counts.pairs_complete >= self.counts.target:
                break
            page = self._id_page(puuid, 0)
            self.counts.ladder_players_used += 1
            before = len(self.known_ids)
            if page is not None:
                self._add_ids(page)
            self.counts.seed_ids_found = len(self.known_ids) - self.counts.snowball_ids_found
            self._save_ids()
            self.fetch_pairs(self.known_ids[before:])
        if self.counts.seed_ids_found == 0:
            raise StopFetch("no_ranked_ids", EXIT_NO_RANKED_IDS, NO_RANKED_IDS_MSG)
        _log(self.log, logging.INFO, "ladder match ids", ids=self.counts.seed_ids_found,
             players=self.counts.ladder_players_used)

    def _snowball_candidates(self, exclude: set[str]) -> list[str]:
        seen = set(exclude)
        out: list[str] = []
        for match_id in self.known_ids:
            if not self.store.has("match", match_id):
                continue
            meta = self.store.read_json(self.store.path("match", match_id)).get("metadata", {})
            for puuid in meta.get("participants", []) if isinstance(meta, dict) else []:
                if isinstance(puuid, str) and puuid not in seen:
                    seen.add(puuid)
                    out.append(puuid)
        return out

    def snowball(self, used: set[str]) -> None:
        if self.counts.pairs_complete >= self.counts.target:
            return
        candidates = self._snowball_candidates(used)
        _log(self.log, logging.INFO, "snowball: ladder players are short of the target",
             pairs=self.counts.pairs_complete, target=self.counts.target,
             candidate_players=len(candidates))
        skip = self.counts.snowball_pages  # pages already used in an earlier run
        for puuid in candidates[skip:]:
            if (self.counts.snowball_pages >= MAX_SNOWBALL_PAGES
                    or self.counts.pairs_complete >= self.counts.target):
                break
            page = self._id_page(puuid, 0)
            self.counts.snowball_pages += 1
            before = len(self.known_ids)
            if page is not None:
                self.counts.snowball_ids_found += self._add_ids(page)
            new_ids = self.known_ids[before:]
            self._save_ids()
            self.fetch_pairs(new_ids)

    def run(self) -> None:
        self.store.ensure()
        self.load_ids()
        puuids = self.ladder_puuids()
        self.fetch_pairs(list(self.known_ids))
        self.ladder_players(puuids)
        self.snowball(set(puuids[: self.counts.ladder_players_used]))


# --- CLI ---------------------------------------------------------------------------------


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Fetch ranked solo matches + timelines (raw).")
    p.add_argument("--platform", required=True, choices=PLATFORMS,
                   help="platform host of the ladder call, e.g. sg2 (UNVERIFIED host list)")
    p.add_argument("--region", required=True, choices=REGIONS,
                   help="regional host for match calls, e.g. sea for sg2 (UNVERIFIED mapping)")
    p.add_argument("--out", default="realdata/raw", help="raw folder (default realdata/raw)")
    p.add_argument("--matches", type=int, default=DEFAULT_MATCHES,
                   help=f"match + timeline pairs to fetch, 1..{DEFAULT_MATCHES}")
    p.add_argument("--max-calls", type=int, default=MAX_CALLS,
                   help=f"hard call cap including retries, 1..{MAX_CALLS}")
    return p.parse_args(argv)


def check_out_dir(out: Path) -> None:
    """Inside the repo, raw data may only go under ``realdata/`` (gitignored)."""
    resolved = out.resolve()
    if resolved.is_relative_to(REPO_ROOT) and not resolved.is_relative_to(ALLOWED_OUT_IN_REPO):
        raise ValueError("inside the repo, --out must be under realdata/ (gitignored)")


def _summary(result: str, exit_code: int, reason: str, message: str,
             client: ApiClient | None, counts: Counts | None, elapsed_s: float) -> dict[str, Any]:
    out: dict[str, Any] = {"result": result, "exit_code": exit_code, "stop_reason": reason,
                           "message": message, "elapsed_s": round(elapsed_s, 1)}
    if counts is not None:
        out.update({k: v for k, v in vars(counts).items()})
    if client is not None:
        out.update({
            "calls": client.calls, "max_calls": client.max_calls,
            "calls_by_endpoint": dict(client.calls_by_endpoint),
            "status_counts": dict(client.status_counts), "retries": client.retries,
            "retry_after_wait_s": client.retry_after_wait_s,
            "failures_in_row_at_end": client.failures_in_row,
            "rate_headers_last_seen": client.rate_headers,
        })
    return out


def run(argv: list[str], *, env: Mapping[str, str], transport: Transport,
        clock: Callable[[], float], sleep: Callable[[float], None],
        stdout: TextIO, stderr: TextIO) -> int:
    """Run the fetch and print one ``SUMMARY {json}`` line on stdout. Returns the exit code."""
    raw_key = env.get(KEY_ENV, "")
    key = raw_key.strip()
    logger = make_logger(stderr, key)
    started = clock()

    def finish(result: str, code: int, reason: str, message: str,
               client: ApiClient | None = None, fetcher: Fetcher | None = None,
               store: RawStore | None = None) -> int:
        level = logging.INFO if code == EXIT_OK else logging.ERROR
        _log(logger, level, message, stop_reason=reason)
        counts = fetcher.counts if fetcher is not None else None
        summary = _summary(result, code, reason, message, client, counts, clock() - started)
        line = mask(json.dumps(summary, default=str), key)
        if store is not None and store.root.is_dir():
            store.write(store.log, (line + "\n").encode("utf-8"))
        stdout.write(f"SUMMARY {line}\n")
        stdout.flush()
        return code

    try:
        args = parse_args(argv)
    except SystemExit as exc:  # argparse already printed usage
        return EXIT_SETUP if exc.code else EXIT_OK
    if not key:
        return finish("SETUP", EXIT_SETUP, "no_key", f"{KEY_ENV} is not set")
    if not 1 <= args.matches <= DEFAULT_MATCHES or not 1 <= args.max_calls <= MAX_CALLS:
        return finish("SETUP", EXIT_SETUP, "bad_limits",
                      f"--matches must be 1..{DEFAULT_MATCHES}, --max-calls 1..{MAX_CALLS}")
    out = Path(args.out)
    try:
        check_out_dir(out)
    except ValueError as exc:
        return finish("SETUP", EXIT_SETUP, "bad_out", str(exc))

    client = ApiClient(key, transport=transport, clock=clock, sleep=sleep, logger=logger,
                       max_calls=args.max_calls)
    store = RawStore(out)
    fetcher = Fetcher(client, store, logger, platform=args.platform, region=args.region,
                      target=args.matches)
    _log(logger, logging.INFO, "start", matches_target=args.matches, platform=args.platform,
         region=args.region, queue=QUEUE_RANKED_SOLO, max_calls=args.max_calls,
         interval_s=MIN_INTERVAL_S,
         max_run_s_without_waits=round(args.max_calls * MIN_INTERVAL_S))
    try:
        fetcher.run()
    except StopFetch as stop:
        return finish("STOPPED" if stop.exit_code != EXIT_SETUP else "SETUP", stop.exit_code,
                      stop.reason, stop.message, client, fetcher, store)
    except KeyboardInterrupt:
        return finish("STOPPED", EXIT_INTERRUPTED, "interrupted", "interrupted (Ctrl-C)",
                      client, fetcher, store)
    if fetcher.counts.pairs_complete < args.matches:
        return finish("STOPPED", EXIT_STOPPED, "short",
                      f"only {fetcher.counts.pairs_complete} of {args.matches} pairs found "
                      "within the page budget", client, fetcher, store)
    return finish("DONE", EXIT_OK, "done",
                  f"done: {fetcher.counts.pairs_complete} match + timeline pairs",
                  client, fetcher, store)


def main() -> int:
    """Entry point with the real clock, sleep and network."""
    return run(sys.argv[1:], env=os.environ, transport=urllib_transport, clock=time.monotonic,
               sleep=time.sleep, stdout=sys.stdout, stderr=sys.stderr)


if __name__ == "__main__":
    sys.exit(main())
