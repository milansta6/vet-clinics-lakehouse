"""Unit tests for the Gold builders (star schema and aggregates)."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from vet_lakehouse.gold import build as G


@pytest.fixture
def clinics(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(
        [
            ("CL001", "Lumipaw Vets Antwerp", "Antwerp", "BE", "Belgium", "EUR", None, 3),
            ("CL010", "Lumipaw Vets Stockholm", "Stockholm", "SE", "Sweden", "SEK", None, 4),
        ],
        "clinic_id string, clinic_name string, city string, country_code string, "
        "country string, currency string, opened_date date, num_vets int",
    )


@pytest.fixture
def invoices(spark: SparkSession) -> DataFrame:
    rows = [
        ("INV-1", "APT-1", "CL001", "PT1", "2025-01-10", "100.00", "EUR", 1.0, "100.00", "paid"),
        ("INV-2", "APT-2", "CL001", "PT2", "2025-01-10", "50.00", "EUR", 1.0, "50.00", "pending"),
        ("INV-3", "APT-3", "CL001", "PT1", "2025-01-10", "70.00", "EUR", 1.0, "70.00", "cancelled"),
        ("INV-4", "APT-4", "CL010", "PT9", "2025-02-03", "1150.00", "SEK", 11.5, "100.00", "paid"),
    ]
    return spark.createDataFrame(
        rows,
        "invoice_id string, appointment_id string, clinic_id string, pet_id string, "
        "invoice_date string, amount_local string, currency string, fx_rate double, "
        "amount_eur string, status string",
    ).select(
        "invoice_id", "appointment_id", "clinic_id", "pet_id",
        F.col("invoice_date").cast("date").alias("invoice_date"),
        F.col("amount_local").cast("decimal(12,2)").alias("amount_local"),
        "currency", "fx_rate",
        F.col("amount_eur").cast("decimal(14,2)").alias("amount_eur"),
        F.lit("card").alias("payment_method"), "status",
    )  # fmt: skip


@pytest.fixture
def dim_pet(spark: SparkSession) -> DataFrame:
    pets = spark.createDataFrame(
        [("PT1", "Luna", "dog", "Beagle", "female", None, "OW1", "CL001", True, 2, None),
         ("PT2", "Milo", "cat", "Siamese", "male", None, "OW2", "CL001", False, 0, None)],
        "pet_id string, name string, species string, breed string, sex string, birth_date date, "
        "owner_id string, home_clinic_id string, is_microchipped boolean, "
        "vaccination_count int, last_vaccination_date date",
    )  # fmt: skip
    owners = spark.createDataFrame(
        [("OW1", "BE", "anna@example.com")], "owner_id string, country_code string, email string"
    )
    return G.build_dim_pet(pets, owners)


def test_dim_date_covers_full_months(invoices: DataFrame) -> None:
    dim_date = G.build_dim_date(invoices, "invoice_date")
    rows = {r["date_key"]: r for r in dim_date.collect()}

    assert len(rows) == 31 + 28, "January and February 2025, no gaps"
    assert min(rows) == 20250101 and max(rows) == 20250228
    saturday = rows[20250111]
    assert (saturday["day_of_week"], saturday["day_name"], saturday["is_weekend"]) == (
        6, "Saturday", True,
    )  # fmt: skip
    monday = rows[20250203]
    assert (monday["day_of_week"], monday["is_weekend"], monday["year_month"]) == (
        1, False, "2025-02",
    )  # fmt: skip
    assert (monday["quarter"], monday["month_name"]) == (1, "February")


def test_dim_pet_has_unknown_member_and_no_owner_pii(dim_pet: DataFrame) -> None:
    rows = {r["pet_id"]: r for r in dim_pet.collect()}

    assert rows["UNKNOWN"]["pet_key"] == G.UNKNOWN_KEY
    assert rows["PT1"]["owner_country_code"] == "BE"
    assert rows["PT2"]["owner_country_code"] is None, "owner not found -> NULL, pet is kept"
    assert "email" not in dim_pet.columns and "owner_id" not in dim_pet.columns
    assert dim_pet.select("pet_key").distinct().count() == 3


def test_dim_clinic_keys_are_stable_and_unique(clinics: DataFrame) -> None:
    first = {r["clinic_id"]: r["clinic_key"] for r in G.build_dim_clinic(clinics).collect()}
    second = {r["clinic_id"]: r["clinic_key"] for r in G.build_dim_clinic(clinics).collect()}
    assert first == second
    assert len(set(first.values())) == 2


def test_fact_invoice_keys(invoices: DataFrame, clinics: DataFrame, dim_pet: DataFrame) -> None:
    fact = G.build_fact_invoice(invoices, dim_pet)
    rows = {r["invoice_id"]: r for r in fact.collect()}
    clinic_keys = {r["clinic_id"]: r["clinic_key"] for r in G.build_dim_clinic(clinics).collect()}
    pet_keys = {r["pet_id"]: r["pet_key"] for r in dim_pet.collect()}

    assert fact.count() == invoices.count(), "one fact row per invoice, nothing dropped"
    assert rows["INV-1"]["date_key"] == 20250110
    assert rows["INV-1"]["clinic_key"] == clinic_keys["CL001"]
    assert rows["INV-1"]["pet_key"] == pet_keys["PT1"]
    assert rows["INV-4"]["pet_key"] == G.UNKNOWN_KEY, "pet PT9 is not in the dimension"
    assert rows["INV-4"]["invoice_year_month"] == "2025-02"


def test_daily_revenue_excludes_cancelled(invoices, clinics, dim_pet) -> None:
    fact = G.build_fact_invoice(invoices, dim_pet)
    daily = G.build_daily_revenue(fact, G.build_dim_clinic(clinics))
    rows = {(str(r["invoice_date"]), r["clinic_id"]): r for r in daily.collect()}

    antwerp = rows[("2025-01-10", "CL001")]
    assert antwerp["invoice_count"] == 2
    assert antwerp["revenue_eur"] == Decimal("150.00")
    assert antwerp["avg_invoice_value_eur"] == Decimal("75.00")
    assert antwerp["country"] == "Belgium"
    assert rows[("2025-02-03", "CL010")]["revenue_eur"] == Decimal("100.00")
    assert len(rows) == 2


def test_avg_invoice_value_monthly(invoices, clinics, dim_pet) -> None:
    fact = G.build_fact_invoice(invoices, dim_pet)
    monthly = G.build_avg_invoice_value_monthly(fact, G.build_dim_clinic(clinics))
    rows = {(r["year_month"], r["country_code"]): r for r in monthly.collect()}

    assert rows[("2025-01", "BE")]["avg_invoice_value_eur"] == Decimal("75.00")
    assert rows[("2025-01", "BE")]["max_invoice_value_eur"] == Decimal("100.00")
    assert rows[("2025-02", "SE")]["invoice_count"] == 1


def test_pets_treated_monthly_counts_distinct_pets(spark: SparkSession, clinics) -> None:
    appointments = spark.createDataFrame(
        [
            ("A1", "CL001", "PT1", date(2025, 1, 10), "completed"),
            ("A2", "CL001", "PT1", date(2025, 1, 20), "completed"),  # same pet, same month
            ("A3", "CL001", "PT2", date(2025, 1, 21), "completed"),
            ("A4", "CL001", "PT3", date(2025, 1, 22), "cancelled"),  # not treated
            ("A5", "CL001", "PT1", date(2025, 2, 1), "completed"),
        ],
        "appointment_id string, clinic_id string, pet_id string, appointment_date date, "
        "status string",
    )
    rows = {
        (r["year_month"], r["clinic_id"]): r
        for r in G.build_pets_treated_monthly(appointments, clinics).collect()
    }
    assert rows[("2025-01", "CL001")]["pets_treated"] == 2
    assert rows[("2025-01", "CL001")]["completed_appointments"] == 3
    assert rows[("2025-02", "CL001")]["pets_treated"] == 1
    assert rows[("2025-01", "CL001")]["country"] == "Belgium"
