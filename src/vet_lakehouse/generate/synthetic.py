"""Synthetic source data for the fictional vet clinic group.

The generator is deterministic: the same ``seed``, ``start`` date and number of
days always give the same files. Each day has its own random seed, so generating
more days later only adds new daily files and leaves old ones unchanged. That is
what makes the incremental load easy to demo.

Data quality problems are added on purpose. Rows with a "hard" problem (they
must end up in quarantine) are counted in ``_manifest.json`` so the end-to-end
test can check that Silver catches exactly those rows.
"""

from __future__ import annotations

import csv
import json
import random
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any

from vet_lakehouse.generate.reference import (
    COUNTRIES,
    FIRST_NAMES,
    GROUP_NAME,
    LAST_NAMES,
    PAYMENT_METHODS,
    PET_NAMES,
    REASONS,
    SPECIES,
    SPECIES_SPELLINGS,
    STREETS,
    VACCINES,
    Country,
)

Record = dict[str, Any]

N_OWNERS = 300
N_PETS = 450
OWNER_UPDATE_OFFSETS = (30,)  # days after start when an owner change file arrives
PET_UPDATE_OFFSETS = (15, 40)  # days after start when a pet change file arrives
PAYMENT_DELAY_DAYS = 3  # pending invoices are re-sent as "paid" this many days later
UNKNOWN_CLINIC_ID = "CL999"

HARD = "_hard_problem"  # private marker: this row must be quarantined by Silver

CLINIC_COLUMNS = [
    "clinic_id", "clinic_name", "city", "country_code", "country", "currency",
    "opened_date", "num_vets",
]  # fmt: skip
APPOINTMENT_COLUMNS = [
    "appointment_id", "clinic_id", "pet_id", "appointment_ts", "reason", "vet_name",
    "status", "duration_min", "updated_at",
]  # fmt: skip
INVOICE_COLUMNS = [
    "invoice_id", "appointment_id", "clinic_id", "pet_id", "invoice_date", "amount",
    "currency", "payment_method", "status", "updated_at",
]  # fmt: skip

TS_FORMAT = "%Y-%m-%d %H:%M:%S"


# --------------------------------------------------------------------------- #
# Small helpers
# --------------------------------------------------------------------------- #
def _iso_z(value: datetime) -> str:
    """Format a timestamp the way a JSON API usually does (UTC, ``Z`` suffix)."""
    return value.strftime("%Y-%m-%dT%H:%M:%SZ")


def _public(row: Record) -> Record:
    """Drop private helper fields (names starting with ``_``) before writing."""
    return {k: v for k, v in row.items() if not k.startswith("_")}


def _write_csv(path: Path, columns: list[str], rows: list[Record]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({c: ("" if row.get(c) is None else row[c]) for c in columns})


def _write_json_lines(path: Path, rows: list[Record]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(_public(row), ensure_ascii=False) + "\n")


def _count_hard(rows: list[Record]) -> int:
    return sum(1 for row in rows if row.get(HARD))


def clinic_index() -> list[tuple[str, Country]]:
    """Return the clean list of (clinic_id, country) used to build other entities."""
    index: list[tuple[str, Country]] = []
    for country in COUNTRIES:
        for _city in country.cities:
            index.append((f"CL{len(index) + 1:03d}", country))
    return index


# --------------------------------------------------------------------------- #
# Clinics (CSV)
# --------------------------------------------------------------------------- #
def build_clinics() -> list[Record]:
    """Build the clinic master data, with messy country codes and mixed date formats."""
    rows: list[Record] = []
    number = 0
    for country in COUNTRIES:
        for city in country.cities:
            number += 1
            opened = date(2005 + (number * 3) % 15, 1 + (number * 5) % 12, 1 + (number * 7) % 27)
            code = country.code
            spellings = (code, f" {code.lower()}", f"{code} ", code.lower())
            rows.append(
                {
                    "clinic_id": f"CL{number:03d}",
                    "clinic_name": f"{GROUP_NAME} Vets {city}",
                    "city": city,
                    "country_code": spellings[number % 4],
                    "country": country.name,
                    "currency": country.currency,
                    "opened_date": opened.strftime("%d/%m/%Y")
                    if number % 3 == 0
                    else opened.isoformat(),
                    "num_vets": 2 + number % 5,
                }
            )
    # A leftover test row from the source system: invalid country code.
    rows.append(
        {
            "clinic_id": "CL900",
            "clinic_name": "TEST CLINIC - DO NOT USE",
            "city": "Nowhere",
            "country_code": "XX",
            "country": "Testland",
            "currency": "EUR",
            "opened_date": "2020-01-01",
            "num_vets": 0,
            HARD: True,
        }
    )
    return rows


# --------------------------------------------------------------------------- #
# Owners (JSON lines, nested contact and address)
# --------------------------------------------------------------------------- #
def _make_owner(rng: random.Random, number: int, registered_at: datetime) -> Record:
    country = rng.choice(COUNTRIES)
    first, last = rng.choice(FIRST_NAMES), rng.choice(LAST_NAMES)
    email: str | None = f"{first}.{last}{number}@example.com".lower().replace(" ", "")
    noise = rng.random()
    if noise < 0.08:
        email = f"  {email.upper()} "  # fixable: Silver trims and lower-cases
    elif noise < 0.11:
        email = None
    elif noise < 0.13:
        email = "not-an-email"  # kept, but flagged with email_valid = false
    phone = f"{country.phone_prefix} {rng.randint(100, 999)} {rng.randint(100000, 999999)}"
    return {
        "owner_id": f"OW{number:05d}",
        "first_name": first,
        "last_name": last,
        "contact": {
            "email": email,
            "phone": phone,
        },
        "address": {
            "street": f"{rng.choice(STREETS)} {rng.randint(1, 180)}",
            "city": rng.choice(country.cities),
            "postal_code": str(rng.randint(1000, 99999)),
            "country_code": country.code,
        },
        "registered_at": _iso_z(registered_at),
        "marketing_consent": rng.random() < 0.4,
        "updated_at": _iso_z(registered_at),
    }


def build_owners(seed: int) -> list[Record]:
    """Build the initial full list of owners (including two rows without an id)."""
    rng = random.Random(f"{seed}-owners")
    owners = []
    for number in range(1, N_OWNERS + 1):
        registered = datetime(2022, 1, 1) + timedelta(
            days=rng.randint(0, 1000), seconds=rng.randint(0, 86399)
        )
        owners.append(_make_owner(rng, number, registered))
    for _ in range(2):
        broken = _make_owner(rng, 0, datetime(2024, 6, 1, 12, 0, 0))
        broken["owner_id"] = None
        broken[HARD] = True
        owners.append(broken)
    return owners


def build_owner_updates(seed: int, base: list[Record], day: date, offset: int) -> list[Record]:
    """Build a change file: some owners moved house, and a few new owners registered."""
    rng = random.Random(f"{seed}-owner-updates-{offset}")
    stamp = datetime.combine(day, time(9, 0))
    valid = [o for o in base if o["owner_id"]]
    changed = []
    for owner in rng.sample(valid, 20):
        updated = json.loads(json.dumps(_public(owner)))  # deep copy
        updated["address"]["street"] = f"{rng.choice(STREETS)} {rng.randint(1, 180)}"
        updated["updated_at"] = _iso_z(stamp)
        changed.append(updated)
    first_new = N_OWNERS + 1 + 10 * OWNER_UPDATE_OFFSETS.index(offset)
    new = [_make_owner(rng, first_new + i, stamp) for i in range(10)]
    return changed + new


# --------------------------------------------------------------------------- #
# Pets (JSON lines, nested microchip object and vaccinations array)
# --------------------------------------------------------------------------- #
def _make_pet(
    rng: random.Random,
    number: int,
    owners: list[Record],
    clinics: list[tuple[str, Country]],
    as_of: date,
) -> Record:
    species = rng.choices(list(SPECIES), weights=[SPECIES[s][0] for s in SPECIES])[0]
    _, breeds, min_kg, max_kg = SPECIES[species]
    owner = rng.choice(owners)
    owner_country = owner["address"]["country_code"]
    home_clinics = [cid for cid, country in clinics if country.code == owner_country]
    birth = as_of - timedelta(days=rng.randint(90, 5500))
    age_days = (as_of - birth).days

    microchip = None
    if rng.random() < 0.8:
        implanted = birth + timedelta(days=rng.randint(30, min(200, age_days)))
        microchip = {
            "number": "".join(str(rng.randint(0, 9)) for _ in range(15)),
            "implanted_on": implanted.isoformat(),
        }
    vaccinations = [
        {
            "vaccine": rng.choice(VACCINES),
            "date": (birth + timedelta(days=rng.randint(30, age_days))).isoformat(),
        }
        for _ in range(rng.randint(0, 3))
    ]
    return {
        "pet_id": f"PT{number:06d}",
        "name": rng.choice(PET_NAMES),
        "species": rng.choice(SPECIES_SPELLINGS[species]),
        "breed": rng.choice(breeds),
        "sex": rng.choice(("M", "F", "male", "female")),
        "birth_date": birth.isoformat(),
        "owner_id": owner["owner_id"],
        "home_clinic_id": rng.choice(home_clinics),
        "microchip": microchip,
        "weight_kg": round(rng.uniform(min_kg, max_kg), 2),
        "vaccinations": vaccinations,
        "updated_at": _iso_z(datetime.combine(as_of, time(6, 0))),
    }


def build_pets(
    seed: int, owners: list[Record], clinics: list[tuple[str, Country]], as_of: date
) -> list[Record]:
    """Build the initial full list of pets, plus eight rows with hard problems."""
    rng = random.Random(f"{seed}-pets")
    valid_owners = [o for o in owners if o["owner_id"]]
    pets = [_make_pet(rng, n, valid_owners, clinics, as_of) for n in range(1, N_PETS + 1)]

    broken_number = 900001
    for problem in ("null_id",) * 3 + ("future_birth",) * 3 + ("negative_weight",) * 2:
        pet = _make_pet(rng, broken_number, valid_owners, clinics, as_of)
        broken_number += 1
        if problem == "null_id":
            pet["pet_id"] = None
        elif problem == "future_birth":
            pet["birth_date"] = "2099-01-01"
        else:
            pet["weight_kg"] = -5.0
        pet[HARD] = True
        pets.append(pet)
    return pets


def build_pet_updates(
    seed: int,
    base: list[Record],
    owners: list[Record],
    clinics: list[tuple[str, Country]],
    day: date,
    offset: int,
) -> list[Record]:
    """Build a change file: weight updates, new vaccinations and a few new pets."""
    rng = random.Random(f"{seed}-pet-updates-{offset}")
    stamp = _iso_z(datetime.combine(day, time(7, 30)))
    valid = [p for p in base if not p.get(HARD)]
    changed = []
    for pet in rng.sample(valid, 25):
        updated = json.loads(json.dumps(_public(pet)))
        updated["weight_kg"] = round(updated["weight_kg"] * rng.uniform(0.9, 1.15), 2)
        updated["vaccinations"].append({"vaccine": rng.choice(VACCINES), "date": day.isoformat()})
        updated["updated_at"] = stamp
        changed.append(updated)
    valid_owners = [o for o in owners if o["owner_id"]]
    first_new = N_PETS + 1 + 8 * PET_UPDATE_OFFSETS.index(offset)
    new = [_make_pet(rng, first_new + i, valid_owners, clinics, day) for i in range(8)]
    return changed + new


# --------------------------------------------------------------------------- #
# Appointments and invoices (daily CSV files)
# --------------------------------------------------------------------------- #
def day_records(
    day: date,
    seed: int,
    clinics: list[tuple[str, Country]],
    pets_by_clinic: dict[str, list[str]],
    all_pet_ids: list[str],
) -> tuple[list[Record], list[Record]]:
    """Build the clean appointments and invoices of one business day.

    The result depends only on the arguments, so any day can be rebuilt later
    (for example to re-send yesterday's rows or to mark old invoices as paid).
    """
    rng = random.Random(f"{seed}-day-{day.isoformat()}")
    reasons = list(REASONS)
    reason_weights = [REASONS[r][0] for r in reasons]
    appointments: list[Record] = []
    invoices: list[Record] = []
    tag = day.strftime("%Y%m%d")

    for clinic_id, country in clinics:
        weekday = day.weekday()
        if weekday < 5:
            visits = rng.randint(4, 8)
        elif weekday == 5:
            visits = rng.randint(1, 3)
        else:
            visits = rng.randint(0, 1)  # Sunday: emergencies only

        for _ in range(visits):
            pet_id = rng.choice(pets_by_clinic.get(clinic_id) or all_pet_ids)
            reason = rng.choices(reasons, weights=reason_weights)[0]
            _, min_price, max_price, duration = REASONS[reason]
            starts_at = datetime.combine(day, time(rng.randint(8, 17), rng.choice((0, 15, 30, 45))))
            status = rng.choices(("completed", "cancelled", "no_show"), weights=(85, 8, 7))[0]
            appointment_id = f"APT-{tag}-{len(appointments) + 1:04d}"
            appointments.append(
                {
                    "appointment_id": appointment_id,
                    "clinic_id": clinic_id,
                    "pet_id": pet_id,
                    "appointment_ts": starts_at.strftime(TS_FORMAT),
                    "reason": reason,
                    "vet_name": f"Dr. {rng.choice(LAST_NAMES)}",
                    "status": status,
                    "duration_min": duration + rng.choice((-5, 0, 0, 5, 10)),
                    "updated_at": datetime.combine(day, time(18, 0)).strftime(TS_FORMAT),
                }
            )
            if status != "completed":
                continue
            price_eur = rng.uniform(min_price, max_price) * country.price_level
            invoice_status = rng.choices(("paid", "pending", "cancelled"), weights=(80, 15, 5))[0]
            invoices.append(
                {
                    "invoice_id": f"INV-{tag}-{len(invoices) + 1:04d}",
                    "appointment_id": appointment_id,
                    "clinic_id": clinic_id,
                    "pet_id": pet_id,
                    "invoice_date": day.isoformat(),
                    "amount": f"{price_eur * country.eur_factor:.2f}",
                    "currency": country.currency,
                    "payment_method": rng.choice(PAYMENT_METHODS),
                    "status": invoice_status,
                    "updated_at": datetime.combine(day, time(18, 30)).strftime(TS_FORMAT),
                    "_paid_later": invoice_status == "pending" and rng.random() < 0.6,
                }
            )
    return appointments, invoices


def _messy_status(rng: random.Random, status: str) -> str:
    """Return the same status written in a slightly different (fixable) way."""
    return rng.choice(
        (status.upper(), status.capitalize(), f" {status} ", status.replace("_", "-"))
    )


def _add_duplicates(rng: random.Random, rows: list[Record], share: float) -> list[Record]:
    """Append exact copies of some rows (duplicates inside the same file)."""
    return rows + [dict(row) for row in rows if rng.random() < share]


def corrupt_appointments(rng: random.Random, rows: list[Record]) -> list[Record]:
    """Add data quality problems to one day of appointments."""
    result = []
    for original in rows:
        row = dict(original)
        roll = rng.random()
        if roll < 0.010:
            row["pet_id"] = None
            row[HARD] = True
        elif roll < 0.015:
            row["appointment_id"] = None
            row[HARD] = True
        elif roll < 0.025:
            # Wrong dates: impossible month/day, or a year far in the future / past.
            clock = row["appointment_ts"][11:]
            row["appointment_ts"] = rng.choice(
                (f"2025-13-45 {clock}", f"2099-01-01 {clock}", f"1900-01-01 {clock}")
            )
            row[HARD] = True
        elif roll < 0.035:
            row["clinic_id"] = UNKNOWN_CLINIC_ID
            row[HARD] = True
        elif roll < 0.060:
            # Fixable: European date format instead of ISO.
            parsed = datetime.strptime(row["appointment_ts"], TS_FORMAT)
            row["appointment_ts"] = parsed.strftime("%d/%m/%Y %H:%M:%S")
        if rng.random() < 0.10:
            row["status"] = _messy_status(rng, row["status"])
        result.append(row)
    return _add_duplicates(rng, result, share=0.02)


def corrupt_invoices(rng: random.Random, rows: list[Record]) -> list[Record]:
    """Add data quality problems to one day of invoices."""
    result = []
    for original in rows:
        row = dict(original)
        roll = rng.random()
        if roll < 0.010:
            row["amount"] = None
            row[HARD] = True
        elif roll < 0.015:
            row["invoice_id"] = None
            row[HARD] = True
        elif roll < 0.025:
            row["amount"] = f"-{row['amount']}"
            row[HARD] = True
        elif roll < 0.035:
            row["invoice_date"] = rng.choice(("2025-13-45", "2099-01-01", "1900-01-01"))
            row[HARD] = True
        elif roll < 0.045:
            row["clinic_id"] = UNKNOWN_CLINIC_ID
            row[HARD] = True
        elif roll < 0.075:
            row["amount"] = row["amount"].replace(".", ",")  # fixable: decimal comma
        elif roll < 0.105:
            row["currency"] = f" {row['currency'].lower()}"  # fixable: case and spaces
        elif roll < 0.125:
            row["currency"] = None  # fixable: Silver falls back to the clinic currency
        elif roll < 0.145:
            parsed = date.fromisoformat(row["invoice_date"])
            row["invoice_date"] = parsed.strftime("%d/%m/%Y")  # fixable: European format
        if rng.random() < 0.10:
            row["status"] = _messy_status(rng, row["status"])
        result.append(row)
    return _add_duplicates(rng, result, share=0.02)


def build_daily_files(
    day: date,
    start: date,
    seed: int,
    clinics: list[tuple[str, Country]],
    pets_by_clinic: dict[str, list[str]],
    all_pet_ids: list[str],
) -> tuple[list[Record], list[Record]]:
    """Build the final (messy) appointment and invoice rows for one daily file."""

    def clean(target: date) -> tuple[list[Record], list[Record]]:
        return day_records(target, seed, clinics, pets_by_clinic, all_pet_ids)

    appointments, invoices = clean(day)
    appointments = corrupt_appointments(random.Random(f"{seed}-dq-appt-{day}"), appointments)
    invoices = corrupt_invoices(random.Random(f"{seed}-dq-inv-{day}"), invoices)

    # Late re-delivery: a few of yesterday's appointments are sent again.
    yesterday = day - timedelta(days=1)
    if yesterday >= start:
        resend = random.Random(f"{seed}-resend-{day}")
        appointments += [dict(a) for a in clean(yesterday)[0] if resend.random() < 0.01]

    # Status change: invoices that were pending are sent again as paid.
    earlier = day - timedelta(days=PAYMENT_DELAY_DAYS)
    if earlier >= start:
        paid_stamp = datetime.combine(day, time(10, 0)).strftime(TS_FORMAT)
        for invoice in clean(earlier)[1]:
            if invoice["_paid_later"]:
                invoices.append({**invoice, "status": "paid", "updated_at": paid_stamp})
    return appointments, invoices


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #
def generate_landing_data(landing: Path | str, start: date, days: int, seed: int = 42) -> Record:
    """Write all synthetic source files to the landing folder.

    Args:
        landing: Target folder. One sub-folder per source is created.
        start: First business day.
        days: Number of daily files to create.
        seed: Random seed. Same inputs always give the same files.

    Returns:
        A manifest with, per source, the number of files and rows written and
        how many rows carry a hard data quality problem.
    """
    landing = Path(landing)
    clinics = clinic_index()
    manifest: Record = {"start_date": start.isoformat(), "days": days, "seed": seed, "sources": {}}

    def record(source: str, files: int, rows: int, hard: int) -> None:
        entry = manifest["sources"].setdefault(source, {"files": 0, "rows": 0, "hard_dq_rows": 0})
        entry["files"] += files
        entry["rows"] += rows
        entry["hard_dq_rows"] += hard

    clinic_rows = build_clinics()
    _write_csv(landing / "clinics" / "clinics.csv", CLINIC_COLUMNS, clinic_rows)
    record("clinics", 1, len(clinic_rows), _count_hard(clinic_rows))

    owners = build_owners(seed)
    _write_json_lines(landing / "owners" / f"owners_{start.isoformat()}.json", owners)
    record("owners", 1, len(owners), _count_hard(owners))
    for offset in OWNER_UPDATE_OFFSETS:
        if offset < days:
            day = start + timedelta(days=offset)
            rows = build_owner_updates(seed, owners, day, offset)
            _write_json_lines(landing / "owners" / f"owners_{day.isoformat()}.json", rows)
            record("owners", 1, len(rows), 0)

    pets = build_pets(seed, owners, clinics, start)
    _write_json_lines(landing / "pets" / f"pets_{start.isoformat()}.json", pets)
    record("pets", 1, len(pets), _count_hard(pets))
    for offset in PET_UPDATE_OFFSETS:
        if offset < days:
            day = start + timedelta(days=offset)
            rows = build_pet_updates(seed, pets, owners, clinics, day, offset)
            _write_json_lines(landing / "pets" / f"pets_{day.isoformat()}.json", rows)
            record("pets", 1, len(rows), 0)

    # Appointments only use the clean pets of the initial file.
    clean_pets = [p for p in pets if not p.get(HARD)]
    all_pet_ids = [p["pet_id"] for p in clean_pets]
    pets_by_clinic: dict[str, list[str]] = {}
    for pet in clean_pets:
        pets_by_clinic.setdefault(pet["home_clinic_id"], []).append(pet["pet_id"])

    for offset in range(days):
        day = start + timedelta(days=offset)
        appointments, invoices = build_daily_files(
            day, start, seed, clinics, pets_by_clinic, all_pet_ids
        )
        _write_csv(
            landing / "appointments" / f"appointments_{day.isoformat()}.csv",
            APPOINTMENT_COLUMNS,
            appointments,
        )
        _write_csv(
            landing / "invoices" / f"invoices_{day.isoformat()}.csv", INVOICE_COLUMNS, invoices
        )
        record("appointments", 1, len(appointments), _count_hard(appointments))
        record("invoices", 1, len(invoices), _count_hard(invoices))

    (landing / "_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest
