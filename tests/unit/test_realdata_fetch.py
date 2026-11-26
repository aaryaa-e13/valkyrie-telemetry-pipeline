"""Unit tests for research/real_match_data/fetch_matches.py (no network, fake clock).

Fixtures are fictional. Names, header names, env var names and URL paths are taken from the
module's constants on purpose: this file must not contain any studio or game name.
"""

from __future__ import annotations

import io
import json
import urllib.parse
from pathlib import Path

import pytest

from research.real_match_data import fetch_matches as fm

KEY = "test-key-0123456789abcdef"
ACCOUNT = "Fictional Hero#TAG1"
SEED = "puuid-seed"


def ok(payload: object, headers: dict[str, str] | None = None) -> fm.HttpResponse:
    return fm.HttpResponse(200, headers or {}, json.dumps(payload).encode("utf-8"))


def status(code: int, headers: dict[str, str] | None = None) -> fm.HttpResponse:
    return fm.HttpResponse(code, headers or {}, b'{"status": "fake"}')


class FakeClock:
    """Time only moves when the code sleeps."""

    def __init__(self) -> None:
        self.t = 1000.0
        self.sleeps: list[float] = []

    def now(self) -> float:
        return self.t

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.t += seconds


class FakeApi:
    """A tiny fake of the four endpoints. ``scripted`` responses are served first."""

    def __init__(self, seed_matches: int = 3, others: dict[str, int] | None = None) -> None:
        self.clock: FakeClock | None = None
        self.ids: dict[str, list[str]] = {
            SEED: [f"XX1_{1000 + i}" for i in range(seed_matches)]}
        for n, (puuid, count) in enumerate((others or {}).items()):
            self.ids[puuid] = [f"XX1_{5000 + 100 * n + i}" for i in range(count)]
        self.scripted: list[fm.HttpResponse | Exception] = []
        self.requests: list[tuple[str, dict[str, str], float]] = []
        self.kinds: list[str] = []

    def participants(self, match_id: str) -> list[str]:
        return [SEED, *[p for p in self.ids if p != SEED]]

    def _kind(self, path: str) -> str:
        if path.startswith(fm.ACCOUNT_PATH.split("{")[0]):
            return "account"
        if path.endswith("/ids"):
            return "match_ids"
        if path.endswith("/timeline"):
            return "timeline"
        return "match"

    def __call__(self, request, timeout: float) -> fm.HttpResponse:
        headers = {k.lower(): v for k, v in request.header_items()}
        when = self.clock.t if self.clock else 0.0
        self.requests.append((request.full_url, headers, when))
        url = urllib.parse.urlsplit(request.full_url)
        kind = self._kind(url.path)
        self.kinds.append(kind)
        if self.scripted:
            item = self.scripted.pop(0)
            if isinstance(item, Exception):
                raise item
            return item
        if kind == "account":
            return ok({"puuid": SEED, "gameName": "Fictional Hero", "tagLine": "TAG1"})
        if kind == "match_ids":
            puuid = urllib.parse.unquote(url.path.split("/")[-2])
            q = dict(urllib.parse.parse_qsl(url.query))
            start, count = int(q["start"]), int(q["count"])
            return ok(self.ids.get(puuid, [])[start:start + count])
        match_id = url.path.split("/")[-2 if kind == "timeline" else -1]
        if kind == "timeline":
            return ok({"metadata": {"matchId": match_id}, "info": {"frames": []}})
        return ok({"metadata": {"matchId": match_id, "participants": self.participants(match_id)},
                   "info": {"queueId": fm.QUEUE_RANKED_SOLO}})


def go(tmp_path: Path, api: FakeApi, *extra: str, env: dict[str, str] | None = None,
       clock: FakeClock | None = None) -> tuple[int, dict, str, FakeClock]:
    clock = clock or FakeClock()
    api.clock = clock
    out, err = io.StringIO(), io.StringIO()
    environ = {fm.KEY_ENV: KEY, fm.ACCOUNT_ENV: ACCOUNT} if env is None else env
    argv = ["--region", "europe", "--out", str(tmp_path / "raw"), *extra]
    code = fm.run(argv, env=environ, transport=api, clock=clock.now, sleep=clock.sleep,
                  stdout=out, stderr=err)
    lines = [ln for ln in out.getvalue().splitlines() if ln.startswith("SUMMARY ")]
    assert len(lines) == 1
    summary = json.loads(lines[0].removeprefix("SUMMARY "))
    assert summary["exit_code"] == code
    return code, summary, out.getvalue() + err.getvalue(), clock


# --- happy path, key handling ------------------------------------------------------------


def test_full_run_fetches_pairs_and_writes_files(tmp_path: Path) -> None:
    api = FakeApi(seed_matches=3)
    code, summary, _, _ = go(tmp_path, api, "--matches", "3")
    assert code == fm.EXIT_OK and summary["result"] == "DONE"
    assert summary["pairs_complete"] == 3 and summary["probe"] == "ok"
    assert summary["calls"] == 1 + 1 + 3 + 3  # account, one id page, 3 matches, 3 timelines
    assert summary["calls_by_endpoint"] == {"account": 1, "match_ids": 1, "match": 3,
                                            "timeline": 3}
    raw = tmp_path / "raw"
    assert len(list((raw / "matches").glob("*.json"))) == 3
    assert len(list((raw / "timelines").glob("*.json"))) == 3
    assert not list(raw.rglob("*.tmp"))
    assert (raw / "fetch_log.json").is_file()


def test_key_only_in_header_never_in_url_or_output(tmp_path: Path) -> None:
    api = FakeApi(seed_matches=2)
    _, _, output, _ = go(tmp_path, api, "--matches", "2")
    for url, headers, _ in api.requests:
        assert headers[fm.TOKEN_HEADER.lower()] == KEY
        assert url.startswith("https://")
        assert KEY not in url
        assert set(dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(url).query))) <= {
            "queue", "start", "count"}
    assert KEY not in output
    for f in (tmp_path / "raw").rglob("*"):
        if f.is_file():
            assert KEY not in f.read_text()


def test_network_error_text_with_key_is_masked(tmp_path: Path) -> None:
    api = FakeApi()
    api.scripted = [fm.TransportError(f"boom {KEY} boom")] * 3
    code, summary, output, _ = go(tmp_path, api)
    assert code == fm.EXIT_STOPPED and summary["stop_reason"] == "account_failed"
    assert KEY not in output and "boom *** boom" in output


def test_mask_replaces_every_copy_and_ignores_empty_secret() -> None:
    assert fm.mask(f"a{KEY}b{KEY}", KEY) == "a***b***"
    assert fm.mask("unchanged", "") == "unchanged"


@pytest.mark.parametrize("env", [{}, {fm.KEY_ENV: ""}, {fm.KEY_ENV: "   "}])
def test_missing_or_empty_key_is_a_setup_error_with_no_call(tmp_path: Path, env) -> None:
    api = FakeApi()
    code, summary, _, _ = go(tmp_path, api, env={**env, fm.ACCOUNT_ENV: ACCOUNT})
    assert code == fm.EXIT_SETUP and summary["stop_reason"] == "no_key"
    assert api.requests == []


def test_bad_account_name_is_a_setup_error_without_echoing_it(tmp_path: Path) -> None:
    api = FakeApi()
    code, summary, output, _ = go(tmp_path, api,
                                  env={fm.KEY_ENV: KEY, fm.ACCOUNT_ENV: "nohashvalue"})
    assert code == fm.EXIT_SETUP and summary["stop_reason"] == "bad_account"
    assert "nohashvalue" not in output and api.requests == []


def test_parse_account_name_splits_on_last_hash() -> None:
    assert fm.parse_account_name(" A b#c#TAG ") == ("A b#c", "TAG")
    for bad in ("", "abc", "#TAG", "name#", "  #  "):
        with pytest.raises(ValueError):
            fm.parse_account_name(bad)


# --- probe -------------------------------------------------------------------------------


def test_probe_success_line(tmp_path: Path) -> None:
    api = FakeApi(seed_matches=1)
    _, _, output, _ = go(tmp_path, api, "--matches", "1")
    probe = [json.loads(ln) for ln in output.splitlines()
             if ln.startswith("{") and "probe ok" in ln]
    assert len(probe) == 1
    assert probe[0]["msg"] == "probe ok: key accepted (HTTP 200)"
    assert probe[0]["call"] == 1 and probe[0]["endpoint"] == "account"


@pytest.mark.parametrize("code_", [401, 403])
def test_probe_rejected_key_stops_after_one_call(tmp_path: Path, code_: int) -> None:
    api = FakeApi()
    api.scripted = [status(code_)]
    code, summary, output, _ = go(tmp_path, api)
    assert code == fm.EXIT_KEY_REJECTED and summary["stop_reason"] == "key_rejected"
    assert summary["calls"] == 1 and len(api.requests) == 1
    assert f"key rejected (HTTP {code_})" in output
    assert summary["probe"] == "rejected"


def test_rejected_key_later_in_the_run_also_stops(tmp_path: Path) -> None:
    api = FakeApi(seed_matches=3)
    api.scripted = [ok({"puuid": SEED, "gameName": "x", "tagLine": "y"}),
                    ok(["XX1_1", "XX1_2"]), status(403)]
    code, summary, _, _ = go(tmp_path, api, "--matches", "2")
    assert code == fm.EXIT_KEY_REJECTED and summary["calls"] == 3


def test_account_not_found_is_a_setup_stop(tmp_path: Path) -> None:
    api = FakeApi()
    api.scripted = [status(404)]
    code, summary, _, _ = go(tmp_path, api)
    assert code == fm.EXIT_SETUP and summary["stop_reason"] == "account_not_found"


# --- pacing, 429, failures, cap ----------------------------------------------------------


def test_calls_are_spaced_by_the_minimum_interval(tmp_path: Path) -> None:
    api = FakeApi(seed_matches=2)
    go(tmp_path, api, "--matches", "2")
    times = [t for _, _, t in api.requests]
    gaps = [b - a for a, b in zip(times, times[1:], strict=False)]
    assert gaps and all(g >= fm.MIN_INTERVAL_S - 1e-9 for g in gaps)


def test_pacing_stays_under_the_candidate_limits() -> None:
    assert fm.max_calls_in_window(1, fm.MIN_INTERVAL_S) <= 20
    assert fm.max_calls_in_window(120, fm.MIN_INTERVAL_S) <= 100
    assert fm.max_calls_in_window(10, fm.MIN_INTERVAL_S) <= 10


def test_429_waits_retry_after_then_retries(tmp_path: Path) -> None:
    api = FakeApi(seed_matches=1)
    api.scripted = [status(429, {"retry-after": "7"})]
    code, summary, _, clock = go(tmp_path, api, "--matches", "1")
    assert code == fm.EXIT_OK
    assert 7.0 in clock.sleeps and summary["retry_after_wait_s"] == 7.0
    assert summary["status_counts"]["429"] == 1 and summary["retries"] == 1


@pytest.mark.parametrize("headers", [{}, {"retry-after": "121"}, {"retry-after": "soon"}])
def test_429_with_missing_or_long_retry_after_stops(tmp_path: Path, headers) -> None:
    api = FakeApi()
    api.scripted = [status(429, headers)]
    code, summary, _, clock = go(tmp_path, api)
    assert code == fm.EXIT_STOPPED and summary["stop_reason"] == "retry_after"
    assert summary["calls"] == 1 and all(s < 121 for s in clock.sleeps)


def test_three_failures_in_a_row_stop(tmp_path: Path) -> None:
    api = FakeApi(seed_matches=5)
    api.scripted = [ok({"puuid": SEED, "gameName": "x", "tagLine": "y"}),
                    ok(["XX1_1", "XX1_2", "XX1_3", "XX1_4"]),
                    status(404), status(404), status(404)]
    code, summary, _, _ = go(tmp_path, api, "--matches", "4")
    assert code == fm.EXIT_STOPPED and summary["stop_reason"] == "failures_in_row"
    assert summary["calls"] == 5


def test_a_success_resets_the_failure_counter(tmp_path: Path) -> None:
    api = FakeApi(seed_matches=3)
    api.scripted = [ok({"puuid": SEED, "gameName": "x", "tagLine": "y"}),
                    ok(["XX1_1", "XX1_2", "XX1_3"]),
                    status(404), status(404),  # two matches skipped
                    ok({"metadata": {"participants": []}}), ok({"info": {}})]
    code, summary, _, _ = go(tmp_path, api, "--matches", "1")
    assert code == fm.EXIT_OK and summary["matches_skipped"] == 2


def test_server_error_is_retried_once(tmp_path: Path) -> None:
    api = FakeApi(seed_matches=1)
    api.scripted = [status(503)]
    code, summary, _, clock = go(tmp_path, api, "--matches", "1")
    assert code == fm.EXIT_OK and summary["retries"] == 1
    assert fm.SERVER_ERROR_WAIT_S in clock.sleeps


def test_call_cap_is_never_exceeded(tmp_path: Path) -> None:
    api = FakeApi(seed_matches=5)
    code, summary, _, _ = go(tmp_path, api, "--matches", "5", "--max-calls", "4")
    assert code == fm.EXIT_STOPPED and summary["stop_reason"] == "call_cap"
    assert len(api.requests) == 4 and summary["calls"] == 4


def test_limits_above_the_plan_are_refused(tmp_path: Path) -> None:
    for extra in (("--matches", "51"), ("--max-calls", str(fm.MAX_CALLS + 1))):
        api = FakeApi()
        code, summary, _, _ = go(tmp_path, api, *extra)
        assert code == fm.EXIT_SETUP and api.requests == []


def test_stricter_live_rate_limit_stops_before_the_next_call(tmp_path: Path) -> None:
    api = FakeApi(seed_matches=2)
    acct = ok({"puuid": SEED, "gameName": "x", "tagLine": "y"},
              {"x-app-rate-limit": "5:10,100:120"})
    api.scripted = [acct]
    code, summary, _, _ = go(tmp_path, api, "--matches", "2")
    assert code == fm.EXIT_STOPPED and summary["stop_reason"] == "rate_limit_stricter"
    assert summary["calls"] == 1


def test_rate_limit_header_parsing() -> None:
    assert fm.parse_rate_limit("20:1,100:120") == [(20, 1.0), (100, 120.0)]
    for bad in ("", "20", "a:b", "0:1", "20:0"):
        assert fm.parse_rate_limit(bad) is None


def test_unparsed_rate_header_is_logged_not_fatal(tmp_path: Path) -> None:
    api = FakeApi(seed_matches=1)
    api.scripted = [ok({"puuid": SEED, "gameName": "x", "tagLine": "y"},
                       {"x-app-rate-limit": "weird"})]
    code, _, output, _ = go(tmp_path, api, "--matches", "1")
    assert code == fm.EXIT_OK and "rate-limit header not understood" in output


# --- ids, snowball, resume ---------------------------------------------------------------


def test_zero_ranked_ids_stops_without_another_seed_method(tmp_path: Path) -> None:
    api = FakeApi(seed_matches=0, others={"puuid-other": 5})
    code, summary, _, _ = go(tmp_path, api)
    assert code == fm.EXIT_NO_RANKED_IDS and summary["stop_reason"] == "no_ranked_ids"
    assert api.kinds == ["account", "match_ids"]
    q = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(api.requests[1][0]).query))
    assert q["queue"] == str(fm.QUEUE_RANKED_SOLO)


def test_zero_ids_on_disk_still_stops_on_a_rerun(tmp_path: Path) -> None:
    api = FakeApi(seed_matches=0)
    go(tmp_path, api)
    api2 = FakeApi(seed_matches=0)
    code, _, _, _ = go(tmp_path, api2)
    assert code == fm.EXIT_NO_RANKED_IDS and api2.requests == []


def test_seed_ids_are_paged_in_twenties_up_to_three_pages(tmp_path: Path) -> None:
    api = FakeApi(seed_matches=70)
    code, summary, _, _ = go(tmp_path, api, "--matches", "50")
    assert code == fm.EXIT_OK
    assert summary["seed_pages"] == 3 and summary["seed_ids_found"] == 60
    assert summary["calls"] == 1 + 3 + 50 + 50


def test_snowball_fills_up_from_other_players(tmp_path: Path) -> None:
    api = FakeApi(seed_matches=2, others={"puuid-a": 2, "puuid-b": 2})
    code, summary, _, _ = go(tmp_path, api, "--matches", "5")
    assert code == fm.EXIT_OK and summary["pairs_complete"] == 5
    assert summary["snowball_pages"] == 2 and summary["snowball_ids_found"] == 4


def test_snowball_uses_at_most_three_pages(tmp_path: Path) -> None:
    others = {f"puuid-{c}": 1 for c in "abcde"}
    api = FakeApi(seed_matches=1, others=others)
    code, summary, _, _ = go(tmp_path, api, "--matches", "10")
    assert code == fm.EXIT_STOPPED and summary["stop_reason"] == "short"
    assert summary["snowball_pages"] == fm.MAX_SNOWBALL_PAGES
    assert summary["pairs_complete"] == 1 + fm.MAX_SNOWBALL_PAGES


def test_rerun_skips_files_on_disk(tmp_path: Path) -> None:
    go(tmp_path, FakeApi(seed_matches=3), "--matches", "3")
    api2 = FakeApi(seed_matches=3)
    code, summary, output, _ = go(tmp_path, api2, "--matches", "3")
    assert code == fm.EXIT_OK and api2.requests == [] and summary["calls"] == 0
    assert "probe skipped" in output


def test_rerun_after_a_stop_fetches_only_what_is_missing(tmp_path: Path) -> None:
    go(tmp_path, FakeApi(seed_matches=3), "--matches", "3", "--max-calls", "5")
    api2 = FakeApi(seed_matches=3)
    code, _, _, _ = go(tmp_path, api2, "--matches", "3")
    assert code == fm.EXIT_OK and len(api2.requests) == 8 - 5


def test_account_file_for_another_account_is_refused(tmp_path: Path) -> None:
    go(tmp_path, FakeApi(seed_matches=1), "--matches", "1")
    api2 = FakeApi()
    code, summary, _, _ = go(tmp_path, api2,
                             env={fm.KEY_ENV: KEY, fm.ACCOUNT_ENV: "Someone Else#XYZ"})
    assert code == fm.EXIT_SETUP and summary["stop_reason"] == "account_mismatch"
    assert api2.requests == []


def test_ids_with_an_unexpected_shape_are_never_used_as_file_names(tmp_path: Path) -> None:
    api = FakeApi(seed_matches=1)
    api.scripted = [ok({"puuid": SEED, "gameName": "x", "tagLine": "y"}),
                    ok(["../../evil", "XX1_1/../x", "XX1_1001"])]
    code, summary, _, _ = go(tmp_path, api, "--matches", "1")
    assert code == fm.EXIT_OK and summary["invalid_ids"] == 2
    names = sorted(p.name for p in (tmp_path / "raw").rglob("*.json"))
    assert names == ["XX1_1001.json", "XX1_1001.json", "account.json", "fetch_log.json",
                     "ids.json"]
    with pytest.raises(ValueError):
        fm.RawStore(tmp_path).path("match", "../x")


# --- summary and output folder -----------------------------------------------------------


def test_summary_holds_counts_only(tmp_path: Path) -> None:
    api = FakeApi(seed_matches=2, others={"puuid-a": 2})
    _, summary, _, _ = go(tmp_path, api, "--matches", "3")
    text = json.dumps(summary)
    for secret_ish in (SEED, "puuid-a", "XX1_", "Fictional Hero", "TAG1", KEY):
        assert secret_ish not in text
    log = (tmp_path / "raw" / "fetch_log.json").read_text()
    assert json.loads(log) == summary


def test_out_dir_inside_the_repo_must_be_under_realdata() -> None:
    fm.check_out_dir(fm.REPO_ROOT / "realdata" / "raw")
    fm.check_out_dir(Path("/tmp/elsewhere"))
    for bad in (fm.REPO_ROOT / "data", fm.REPO_ROOT / "research", fm.REPO_ROOT):
        with pytest.raises(ValueError):
            fm.check_out_dir(bad)


def test_build_url_is_https_and_rejects_unknown_regions() -> None:
    assert fm.build_url("europe", "/x", {"a": 1}).startswith("https://europe.")
    with pytest.raises(ValueError):
        fm.build_url("evil.example.com/", "/x")


def test_redirects_are_refused() -> None:
    handler = fm._NoRedirect()
    assert handler.redirect_request(None, None, 302, "Found", {}, "https://elsewhere/") is None


def test_parse_retry_after() -> None:
    assert fm.parse_retry_after("3") == 3.0
    for bad in (None, "", "x", "-1", "nan", "inf"):
        assert fm.parse_retry_after(bad) is None


# --- the real urllib transport, against a local server (127.0.0.1 only) ------------------


@pytest.fixture()
def local_server():
    import http.server
    import threading

    seen: list[dict[str, str]] = []

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802 (http.server API)
            seen.append({k.lower(): v for k, v in self.headers.items()})
            routes = {"/ok": (200, {}, b'{"a": 1}'),
                      "/limited": (429, {"Retry-After": "4", "X-App-Rate-Limit": "20:1"}, b"{}"),
                      "/moved": (302, {"Location": "/ok"}, b"")}
            code, headers, body = routes[self.path]
            self.send_response(code)
            for k, v in headers.items():
                self.send_header(k, v)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args) -> None:
            pass

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_address[1]}", seen
    server.shutdown()
    server.server_close()


def _req(url: str):
    import urllib.request

    return urllib.request.Request(url, headers={fm.TOKEN_HEADER: KEY}, method="GET")


def test_transport_200_and_http_errors_become_responses(local_server) -> None:
    base, seen = local_server
    r = fm.urllib_transport(_req(base + "/ok"), 5)
    assert r.status == 200 and json.loads(r.body) == {"a": 1}
    r = fm.urllib_transport(_req(base + "/limited"), 5)
    assert r.status == 429 and r.headers["retry-after"] == "4"
    assert r.headers["x-app-rate-limit"] == "20:1"
    assert seen[0][fm.TOKEN_HEADER.lower()] == KEY


def test_transport_does_not_follow_redirects(local_server) -> None:
    base, seen = local_server
    r = fm.urllib_transport(_req(base + "/moved"), 5)
    assert r.status == 302 and len(seen) == 1  # the key was sent once, not forwarded


def test_transport_network_error_has_no_header_values() -> None:
    import socket

    with socket.socket() as s:  # a port that is closed right after
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    with pytest.raises(fm.TransportError) as info:
        fm.urllib_transport(_req(f"http://127.0.0.1:{port}/x"), 2)
    assert KEY not in str(info.value)
