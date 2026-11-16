"""Tests for the synthetic data generator (plain Python, no Spark needed)."""

from __future__ import annotations

import csv
import json
from datetime import date
from pathlib import Path

from vet_lakehouse.generate.synthetic import (
    APPOINTMENT_COLUMNS,
    INVOICE_COLUMNS,
    UNKNOWN_CLINIC_ID,
    build_clinics,
    generate_landing_data,
)

START = date(2025, 1, 1)


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _all_rows(folder: Path) -> list[dict[str, str]]:
    return [row for path in sorted(folder.glob("*.csv")) for row in _read_csv(path)]


def test_generator_writes_all_sources(tmp_path: Path) -> None:
    manifest = generate_landing_data(tmp_path, START, days=5)

    assert (tmp_path / "clinics" / "clinics.csv").is_file()
    assert (tmp_path / "owners" / "owners_2025-01-01.json").is_file()
    assert (tmp_path / "pets" / "pets_2025-01-01.json").is_file()
    assert len(list((tmp_path / "appointments").glob("*.csv"))) == 5
    assert len(list((tmp_path / "invoices").glob("*.csv"))) == 5
    assert manifest["sources"]["invoices"]["files"] == 5
    assert json.loads((tmp_path / "_manifest.json").read_text())["days"] == 5


def test_generator_is_deterministic(tmp_path: Path) -> None:
    generate_landing_data(tmp_path / "a", START, days=4, seed=7)
    generate_landing_data(tmp_path / "b", START, days=4, seed=7)

    for file_a in sorted((tmp_path / "a").rglob("*.*")):
        file_b = tmp_path / "b" / file_a.relative_to(tmp_path / "a")
        assert file_a.read_bytes() == file_b.read_bytes(), file_a.name


def test_more_days_only_adds_files(tmp_path: Path) -> None:
    """Old daily files must stay identical, otherwise the incremental demo is not honest."""
    generate_landing_data(tmp_path, START, days=4)
    before = {p.name: p.read_bytes() for p in (tmp_path / "invoices").glob("*.csv")}

    generate_landing_data(tmp_path, START, days=7)
    after = {p.name: p.read_bytes() for p in (tmp_path / "invoices").glob("*.csv")}

    assert len(after) == 7
    assert all(after[name] == content for name, content in before.items())


def test_csv_headers_match_the_bronze_contract(tmp_path: Path) -> None:
    generate_landing_data(tmp_path, START, days=1)
    appointments = _read_csv(tmp_path / "appointments" / "appointments_2025-01-01.csv")
    invoices = _read_csv(tmp_path / "invoices" / "invoices_2025-01-01.csv")
    assert list(appointments[0]) == APPOINTMENT_COLUMNS
    assert list(invoices[0]) == INVOICE_COLUMNS


def test_deliberate_data_quality_problems_are_present(tmp_path: Path) -> None:
    manifest = generate_landing_data(tmp_path, START, days=30)
    invoices = _all_rows(tmp_path / "invoices")
    appointments = _all_rows(tmp_path / "appointments")

    invoice_ids = [r["invoice_id"] for r in invoices if r["invoice_id"]]
    assert len(invoice_ids) > len(set(invoice_ids)), "expected duplicate invoice ids"
    assert any(r["amount"] == "" for r in invoices), "expected missing amounts"
    assert any(r["amount"].startswith("-") for r in invoices), "expected negative amounts"
    assert any(r["invoice_date"] in ("2025-13-45", "2099-01-01") for r in invoices)
    assert any(r["clinic_id"] == UNKNOWN_CLINIC_ID for r in invoices)
    assert any(r["pet_id"] == "" for r in appointments), "expected missing pet ids"
    assert any(r["clinic_id"] == UNKNOWN_CLINIC_ID for r in appointments)

    assert manifest["sources"]["invoices"]["hard_dq_rows"] > 0
    assert manifest["sources"]["appointments"]["hard_dq_rows"] > 0
    assert manifest["sources"]["pets"]["hard_dq_rows"] == 8
    assert manifest["sources"]["owners"]["hard_dq_rows"] == 2
    assert manifest["sources"]["clinics"]["hard_dq_rows"] == 1


def test_pending_invoices_are_sent_again_as_paid(tmp_path: Path) -> None:
    generate_landing_data(tmp_path, START, days=10)
    versions: dict[str, set[str]] = {}
    for row in _all_rows(tmp_path / "invoices"):
        if row["invoice_id"]:
            versions.setdefault(row["invoice_id"], set()).add(row["status"].strip().lower())
    assert any({"pending", "paid"} <= statuses for statuses in versions.values())


def test_pets_json_is_nested(tmp_path: Path) -> None:
    generate_landing_data(tmp_path, START, days=1)
    lines = (tmp_path / "pets" / "pets_2025-01-01.json").read_text(encoding="utf-8").splitlines()
    pets = [json.loads(line) for line in lines]

    assert any(isinstance(p["microchip"], dict) for p in pets)
    assert any(p["microchip"] is None for p in pets)
    assert all(isinstance(p["vaccinations"], list) for p in pets)
    assert not any(key.startswith("_") for p in pets for key in p), "private fields leaked"


def test_clinics_cover_five_countries() -> None:
    clinics = [c for c in build_clinics() if c["country_code"].strip().upper() != "XX"]
    assert {c["country_code"].strip().upper() for c in clinics} == {"BE", "NL", "DE", "SE", "RS"}
    assert {c["currency"] for c in clinics} == {"EUR", "SEK", "RSD"}
