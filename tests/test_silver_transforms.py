"""Unit tests for the Silver transformations, using a handful of rows per case."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from vet_lakehouse.bronze import schemas as S
from vet_lakehouse.silver import transforms as T


@pytest.fixture
def clinics(spark: SparkSession) -> DataFrame:
    """Silver clinics: one euro clinic and one Swedish clinic."""
    return spark.createDataFrame(
        [("CL001", "EUR"), ("CL010", "SEK")], "clinic_id string, currency string"
    )


@pytest.fixture
def fx_rates(spark: SparkSession) -> DataFrame:
    """Silver FX rates for one day."""
    return spark.createDataFrame(
        [("2025-01-10", "EUR", 1.0), ("2025-01-10", "SEK", 11.5)],
        "rate_date string, currency string, rate_per_eur double",
    ).withColumn("rate_date", F.col("rate_date").cast("date"))


def _invoice(**overrides: str | None) -> dict[str, str | None]:
    row = {
        "invoice_id": "INV-1",
        "appointment_id": "APT-1",
        "clinic_id": "CL001",
        "pet_id": "PT000001",
        "invoice_date": "2025-01-10",
        "amount": "100.00",
        "currency": "EUR",
        "payment_method": "card",
        "status": "paid",
        "updated_at": "2025-01-10 18:30:00",
    }
    return {**row, **overrides}


def _by_id(df: DataFrame, key: str) -> dict[str, dict]:
    return {row[key]: row.asDict(recursive=True) for row in df.collect()}


# --------------------------------------------------------------------------- #
# Column helpers
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("2025-01-31", date(2025, 1, 31)),
        (" 2025-01-31 ", date(2025, 1, 31)),
        ("31/01/2025", date(2025, 1, 31)),
        ("2025-13-45", None),
        ("31/02/2025", None),
        ("yesterday", None),
        (None, None),
    ],
)
def test_parse_date(spark: SparkSession, text: str | None, expected: date | None) -> None:
    df = spark.createDataFrame([(text,)], "value string")
    assert df.select(T.parse_date(F.col("value")).alias("d")).first()["d"] == expected


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("2025-01-31 09:15:00", datetime(2025, 1, 31, 9, 15)),
        ("2025-01-31T09:15:00Z", datetime(2025, 1, 31, 9, 15)),
        ("31/01/2025 09:15:00", datetime(2025, 1, 31, 9, 15)),
        ("2025-13-45 09:15:00", None),
        ("", None),
    ],
)
def test_parse_timestamp(spark: SparkSession, text: str, expected: datetime | None) -> None:
    df = spark.createDataFrame([(text,)], "value string")
    # Compare as text so the test does not depend on the time zone of the machine.
    result = df.select(
        F.date_format(T.parse_timestamp(F.col("value")), "yyyy-MM-dd HH:mm:ss").alias("ts")
    ).first()["ts"]
    assert result == (expected.strftime("%Y-%m-%d %H:%M:%S") if expected else None)


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("12.50", Decimal("12.50")),
        ("12,50", Decimal("12.50")),
        (" 7 ", Decimal("7.00")),
        ("-3.2", Decimal("-3.20")),
        ("abc", None),
        ("1.2.3", None),
        (None, None),
    ],
)
def test_parse_decimal(spark: SparkSession, text: str | None, expected: Decimal | None) -> None:
    df = spark.createDataFrame([(text,)], "value string")
    assert df.select(T.parse_decimal(F.col("value")).alias("n")).first()["n"] == expected


def test_standardise_status_and_species(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [("No-Show", " DOG "), ("COMPLETED ", "feline"), ("weird", "hamster"), (None, None)],
        "status string, species string",
    )
    rows = df.select(
        T.standardise_status(F.col("status"), T.APPOINTMENT_STATUSES).alias("status"),
        T.standardise_species(F.col("species")).alias("species"),
    ).collect()
    assert [(r["status"], r["species"]) for r in rows] == [
        ("no_show", "dog"),
        ("completed", "cat"),
        ("unknown", "other"),
        ("unknown", None),
    ]


# --------------------------------------------------------------------------- #
# Invoices
# --------------------------------------------------------------------------- #
def test_invoices_are_typed_and_converted_to_eur(bronze_df, clinics, fx_rates) -> None:
    bronze = bronze_df(
        S.INVOICES_SCHEMA,
        [
            _invoice(invoice_id="INV-EUR"),
            _invoice(invoice_id="INV-SEK", clinic_id="CL010", currency="SEK", amount="1150.00"),
        ],
    )
    rows = _by_id(T.transform_invoices(bronze, clinics, fx_rates), "invoice_id")

    assert rows["INV-EUR"]["amount_eur"] == Decimal("100.00")
    assert rows["INV-SEK"]["amount_local"] == Decimal("1150.00")
    assert rows["INV-SEK"]["fx_rate"] == 11.5
    assert rows["INV-SEK"]["amount_eur"] == Decimal("100.00")
    assert rows["INV-SEK"]["invoice_date"] == date(2025, 1, 10)
    assert all(row["_dq_failures"] == [] for row in rows.values())


def test_invoices_fixable_problems_are_repaired(bronze_df, clinics, fx_rates) -> None:
    bronze = bronze_df(
        S.INVOICES_SCHEMA,
        [
            _invoice(invoice_id="COMMA", amount="99,90"),
            _invoice(invoice_id="EU-DATE", invoice_date="10/01/2025"),
            _invoice(invoice_id="LOWER", clinic_id=" cl010 ", currency=" sek", status=" PAID "),
            _invoice(invoice_id="NO-CURRENCY", clinic_id="CL010", currency=None),
        ],
    )
    rows = _by_id(T.transform_invoices(bronze, clinics, fx_rates), "invoice_id")

    assert rows["COMMA"]["amount_local"] == Decimal("99.90")
    assert rows["EU-DATE"]["invoice_date"] == date(2025, 1, 10)
    assert (rows["LOWER"]["clinic_id"], rows["LOWER"]["currency"]) == ("CL010", "SEK")
    assert rows["LOWER"]["status"] == "paid"
    assert rows["NO-CURRENCY"]["currency"] == "SEK", "falls back to the clinic currency"
    assert all(row["_dq_failures"] == [] for row in rows.values())


def test_invoices_bad_rows_get_the_right_rule_names(bronze_df, clinics, fx_rates) -> None:
    bronze = bronze_df(
        S.INVOICES_SCHEMA,
        [
            _invoice(invoice_id="NEGATIVE", amount="-50.00"),
            _invoice(invoice_id="NO-AMOUNT", amount=None),
            _invoice(invoice_id="BAD-DATE", invoice_date="2025-13-45"),
            _invoice(invoice_id="FUTURE", invoice_date="2099-01-01"),
            _invoice(invoice_id="UNKNOWN-CLINIC", clinic_id="CL999"),
            _invoice(invoice_id="NO-RATE", invoice_date="2025-01-11"),
            _invoice(invoice_id=None),
        ],
    )
    checked = T.transform_invoices(bronze, clinics, fx_rates)
    failures = {r["invoice_id"]: set(r["_dq_failures"]) for r in checked.collect()}

    assert failures["NEGATIVE"] == {"amount_not_negative"}
    assert failures["NO-AMOUNT"] == {"amount_not_null"}
    assert failures["BAD-DATE"] == {"invoice_date_valid"}
    assert failures["FUTURE"] == {"invoice_date_in_range", "fx_rate_available"}
    assert failures["UNKNOWN-CLINIC"] == {"clinic_id_known"}
    assert failures["NO-RATE"] == {"fx_rate_available"}
    assert failures[None] == {"invoice_id_not_null"}


def test_split_and_quarantine_shape(bronze_df, clinics, fx_rates) -> None:
    bronze = bronze_df(
        S.INVOICES_SCHEMA, [_invoice(invoice_id="GOOD"), _invoice(invoice_id="BAD", amount="-1")]
    )
    valid, rejected = T.split_valid_rejected(T.transform_invoices(bronze, clinics, fx_rates))
    quarantine = T.to_quarantine(rejected, "invoices", "invoice_id")

    assert [r["invoice_id"] for r in valid.collect()] == ["GOOD"]
    assert "_dq_failures" not in valid.columns and "_raw" not in valid.columns
    row = quarantine.first()
    assert (row["source_table"], row["record_key"]) == ("invoices", "BAD")
    assert row["failed_rules"] == ["amount_not_negative"]
    assert '"amount":"-1"' in row["raw_record"], "the raw value must be kept for debugging"
    assert row["_batch_id"] == "batch-1"


def test_deduplicate_latest_keeps_newest_version(bronze_df, clinics, fx_rates) -> None:
    bronze = bronze_df(
        S.INVOICES_SCHEMA,
        [
            _invoice(status="pending", updated_at="2025-01-10 18:30:00"),
            _invoice(status="pending", updated_at="2025-01-10 18:30:00"),  # exact duplicate
            _invoice(status="paid", updated_at="2025-01-13 10:00:00"),  # newer version
            _invoice(invoice_id="INV-2"),
        ],
    )
    valid, _ = T.split_valid_rejected(T.transform_invoices(bronze, clinics, fx_rates))
    rows = _by_id(T.deduplicate_latest(valid, ["invoice_id"]), "invoice_id")

    assert set(rows) == {"INV-1", "INV-2"}
    assert rows["INV-1"]["status"] == "paid"


# --------------------------------------------------------------------------- #
# Appointments, clinics, owners, pets
# --------------------------------------------------------------------------- #
def test_appointments_rules(bronze_df, clinics) -> None:
    base = {
        "clinic_id": "CL001",
        "pet_id": "PT000001",
        "appointment_ts": "2025-01-10 09:00:00",
        "reason": "Checkup",
        "status": "Completed",
        "duration_min": "20",
        "updated_at": "2025-01-10 18:00:00",
    }
    bronze = bronze_df(
        S.APPOINTMENTS_SCHEMA,
        [
            {**base, "appointment_id": "OK"},
            {**base, "appointment_id": "EU", "appointment_ts": "10/01/2025 09:00:00"},
            {**base, "appointment_id": "NO-PET", "pet_id": None},
            {**base, "appointment_id": "BAD-TS", "appointment_ts": "2025-13-45 09:00:00"},
            {**base, "appointment_id": "OLD", "appointment_ts": "1900-01-01 09:00:00"},
            {**base, "appointment_id": "UNKNOWN-CLINIC", "clinic_id": "CL999"},
        ],
    )
    rows = _by_id(T.transform_appointments(bronze, clinics), "appointment_id")

    assert rows["OK"]["_dq_failures"] == []
    assert rows["OK"]["status"] == "completed" and rows["OK"]["reason"] == "checkup"
    assert rows["OK"]["duration_min"] == 20
    assert rows["EU"]["appointment_date"] == date(2025, 1, 10)
    assert rows["NO-PET"]["_dq_failures"] == ["pet_id_not_null"]
    assert rows["BAD-TS"]["_dq_failures"] == ["appointment_ts_valid"]
    assert rows["OLD"]["_dq_failures"] == ["appointment_ts_in_range"]
    assert rows["UNKNOWN-CLINIC"]["_dq_failures"] == ["clinic_id_known"]


def test_clinics_are_standardised(bronze_df) -> None:
    bronze = bronze_df(
        S.CLINICS_SCHEMA,
        [
            {"clinic_id": "CL001", "country_code": " be", "currency": "EUR",
             "opened_date": "15/03/2010", "num_vets": "4"},
            {"clinic_id": "CL900", "country_code": "XX", "currency": "EUR"},
        ],
    )  # fmt: skip
    rows = _by_id(T.transform_clinics(bronze), "clinic_id")

    assert rows["CL001"]["country_code"] == "BE"
    assert rows["CL001"]["opened_date"] == date(2010, 3, 15)
    assert rows["CL001"]["num_vets"] == 4
    assert rows["CL001"]["_dq_failures"] == []
    assert rows["CL900"]["_dq_failures"] == ["country_code_valid"]


def test_owners_nested_json_is_flattened(bronze_df) -> None:
    bronze = bronze_df(
        S.OWNERS_SCHEMA,
        [
            {
                "owner_id": "OW00001",
                "first_name": "Anna",
                "contact": {"email": "  ANNA.BERG@EXAMPLE.COM ", "phone": "+46 123 456789"},
                "address": {"street": "Mill Street 1", "city": "Stockholm", "country_code": "se"},
                "registered_at": "2024-05-01T10:00:00Z",
                "marketing_consent": "true",
                "updated_at": "2024-05-01T10:00:00Z",
            },
            {"owner_id": "OW00002", "contact": {"email": "not-an-email"}},
            {"owner_id": None, "first_name": "Ghost"},
        ],
    )
    rows = _by_id(T.transform_owners(bronze), "owner_id")

    anna = rows["OW00001"]
    assert anna["email"] == "anna.berg@example.com" and anna["email_valid"] is True
    assert (anna["city"], anna["country_code"]) == ("Stockholm", "SE")
    assert anna["marketing_consent"] is True
    assert rows["OW00002"]["email_valid"] is False
    assert rows["OW00002"]["_dq_failures"] == [], "a bad email is flagged, not quarantined"
    assert rows[None]["_dq_failures"] == ["owner_id_not_null"]


def test_pets_nested_json_is_flattened(bronze_df) -> None:
    bronze = bronze_df(
        S.PETS_SCHEMA,
        [
            {
                "pet_id": "PT000001",
                "name": "luna",
                "species": " DOG ",
                "sex": "F",
                "birth_date": "2020-06-01",
                "owner_id": "OW00001",
                "home_clinic_id": "cl001",
                "microchip": {"number": "123456789012345", "implanted_on": "2020-08-01"},
                "weight_kg": "12.5",
                "vaccinations": [
                    {"vaccine": "Rabies", "date": "2021-06-01"},
                    {"vaccine": "Distemper", "date": "2023-06-01"},
                ],
                "updated_at": "2025-01-01T06:00:00Z",
            },
            {"pet_id": "PT000002", "species": "feline", "vaccinations": [], "weight_kg": "4"},
            {"pet_id": "FUTURE", "birth_date": "2099-01-01"},
            {"pet_id": "NEGATIVE", "weight_kg": "-5.0"},
            {"pet_id": None, "name": "Nobody"},
        ],
    )
    rows = _by_id(T.transform_pets(bronze), "pet_id")

    luna = rows["PT000001"]
    assert (luna["name"], luna["species"], luna["sex"]) == ("Luna", "dog", "female")
    assert luna["home_clinic_id"] == "CL001"
    assert luna["is_microchipped"] is True
    assert luna["microchip_implanted_on"] == date(2020, 8, 1)
    assert luna["weight_kg"] == 12.5
    assert luna["vaccination_count"] == 2
    assert luna["last_vaccination_date"] == date(2023, 6, 1)
    assert luna["vaccinations"][0] == {"vaccine": "Rabies", "date": date(2021, 6, 1)}
    assert luna["_dq_failures"] == []

    cat = rows["PT000002"]
    assert (cat["species"], cat["is_microchipped"], cat["vaccination_count"]) == ("cat", False, 0)
    assert rows["FUTURE"]["_dq_failures"] == ["birth_date_not_in_future"]
    assert rows["NEGATIVE"]["_dq_failures"] == ["weight_in_range"]
    assert rows[None]["_dq_failures"] == ["pet_id_not_null"]


# --------------------------------------------------------------------------- #
# FX rates
# --------------------------------------------------------------------------- #
def test_fx_rates_are_exploded_and_forward_filled(spark: SparkSession, bronze_df) -> None:
    # Friday 2025-01-03 and Monday 2025-01-06 are published; the weekend is missing.
    api = spark.createDataFrame(
        [
            (
                {"2025-01-03": {"SEK": 11.40}, "2025-01-06": {"SEK": 11.50}},
                "2026-01-15 08:00:00",
            )
        ],
        "rates map<string,map<string,double>>, _ingested_at string",
    ).withColumn("_ingested_at", F.col("_ingested_at").cast("timestamp"))
    manual = bronze_df(
        S.MANUAL_FX_SCHEMA,
        [{"currency": "rsd", "valid_from": "2025-01-06", "rate_per_eur": "117.10"}],
    )
    rates = T.build_fx_rates(api, manual, max_forward_fill_days=2)
    rows = {(str(r["rate_date"]), r["currency"]): r for r in rates.collect()}

    assert rows[("2025-01-03", "SEK")]["rate_per_eur"] == 11.40
    assert rows[("2025-01-04", "SEK")]["rate_per_eur"] == 11.40
    assert rows[("2025-01-05", "SEK")]["is_forward_filled"] is True
    assert rows[("2025-01-06", "SEK")]["rate_per_eur"] == 11.50
    assert rows[("2025-01-06", "SEK")]["is_forward_filled"] is False
    assert rows[("2025-01-06", "SEK")]["source"] == "frankfurter_ecb"
    # The fill continues for max_forward_fill_days after the last published rate, not longer.
    assert rows[("2025-01-08", "SEK")]["rate_per_eur"] == 11.50
    assert ("2025-01-09", "SEK") not in rows
    # Manual currency and the EUR identity rate are included.
    assert rows[("2025-01-07", "RSD")]["rate_per_eur"] == 117.10
    assert ("2025-01-05", "RSD") not in rows, "no rate before the first manual rate"
    assert rows[("2025-01-04", "EUR")]["rate_per_eur"] == 1.0
    # One row per day and currency.
    assert rates.count() == rates.select("rate_date", "currency").distinct().count()


def test_fx_rates_latest_delivery_wins(spark: SparkSession, bronze_df) -> None:
    api = spark.createDataFrame(
        [
            ({"2025-01-03": {"SEK": 11.40}}, "2026-01-15 08:00:00"),
            ({"2025-01-03": {"SEK": 11.45}}, "2026-01-16 08:00:00"),  # corrected later
        ],
        "rates map<string,map<string,double>>, _ingested_at string",
    ).withColumn("_ingested_at", F.col("_ingested_at").cast("timestamp"))
    manual = bronze_df(S.MANUAL_FX_SCHEMA, [])
    rates = T.build_fx_rates(api, manual, max_forward_fill_days=0)

    sek = rates.filter("currency = 'SEK'").collect()
    assert len(sek) == 1 and sek[0]["rate_per_eur"] == 11.45
