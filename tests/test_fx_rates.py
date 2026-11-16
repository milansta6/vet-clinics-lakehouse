"""Tests for the FX rate ingestion. The real API is never called here."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import pytest
import requests

from vet_lakehouse.generate import fx_rates

START, END = date(2025, 1, 1), date(2025, 1, 31)


class _FakeResponse:
    def __init__(self, payload: dict[str, Any]) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, Any]:
        return self._payload


def test_cached_sample_has_the_api_shape() -> None:
    sample = json.loads(fx_rates.DEFAULT_SAMPLE_PATH.read_text(encoding="utf-8"))
    assert sample["base"] == "EUR"
    assert sample["start_date"] <= "2025-01-01" and sample["end_date"] >= "2025-03-31"
    assert all(day["SEK"] > 0 for day in sample["rates"].values())


def test_offline_mode_uses_the_sample(tmp_path: Path) -> None:
    assert fx_rates.write_fx_rates(tmp_path, START, END, live=False) == "sample"
    written = json.loads((tmp_path / "fx_rates" / "fx_rates_sample.json").read_text())
    assert "rates" in written


def test_api_failure_falls_back_to_the_sample(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def broken_get(*args: Any, **kwargs: Any) -> None:
        raise requests.ConnectionError("no network")

    monkeypatch.setattr(fx_rates.requests, "get", broken_get)
    assert fx_rates.write_fx_rates(tmp_path, START, END, live=True) == "sample"
    assert (tmp_path / "fx_rates" / "fx_rates_sample.json").is_file()


def test_live_answer_is_saved_as_raw_json(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    payload = {"amount": 1.0, "base": "EUR", "rates": {"2025-01-02": {"SEK": 11.42}}}
    calls: list[dict[str, Any]] = []

    def fake_get(url: str, **kwargs: Any) -> _FakeResponse:
        calls.append({"url": url, **kwargs})
        return _FakeResponse(payload)

    monkeypatch.setattr(fx_rates.requests, "get", fake_get)
    assert fx_rates.write_fx_rates(tmp_path, START, END, live=True) == "live"

    saved = tmp_path / "fx_rates" / "fx_rates_2025-01-01_2025-01-31.json"
    assert json.loads(saved.read_text()) == payload
    assert calls[0]["params"] == {"from": "EUR", "to": "SEK"}
    assert "2024-12-25..2025-01-31" in calls[0]["url"], "should ask for a few earlier days"


def test_empty_api_answer_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(fx_rates.requests, "get", lambda *a, **k: _FakeResponse({"rates": {}}))
    with pytest.raises(ValueError):
        fx_rates.fetch_fx_rates(START, END)


def test_manual_rates_are_weekly_and_cover_the_range() -> None:
    rows = fx_rates.build_manual_rates(START, END)
    days = [date.fromisoformat(r["valid_from"]) for r in rows]

    assert all(d.weekday() == 0 for d in days), "manual rates are published on Mondays"
    assert min(days) <= START
    assert (END - max(days)).days < 7
    assert all(r["currency"] == "RSD" and float(r["rate_per_eur"]) > 100 for r in rows)
