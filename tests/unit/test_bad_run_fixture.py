"""tests/fixtures/bad_run.json is exactly what the real simulator code builds today."""

from __future__ import annotations

from common.manifest_reader import parse_manifest
from tests.fixtures import build_bad_run as b


def test_the_checked_in_fixture_matches_the_code() -> None:
    assert b.PATH.read_text(encoding="utf-8") == b.dumps(b.build()), (
        "regenerate: .venv/bin/python -m tests.fixtures.build_bad_run")


def test_the_fixture_covers_every_gate_and_reads() -> None:
    import json

    data = json.loads(b.PATH.read_text(encoding="utf-8"))
    m = parse_manifest(data["manifest"])
    assert m.version == 4 and m.bad_data is not None and m.bad_data.bad_events is not None
    kinds = {x["bad_kind"] for x in data["messages"] if x["bad_kind"]}
    assert {"epoch_time", "future_time", "malformed_json"} <= kinds
    assert len(m.bad_data.bad_events) == len(
        [x for x in data["messages"] if x["bad_kind"]])
