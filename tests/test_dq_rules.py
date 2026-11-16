"""Unit tests for the reusable data quality rules."""

from __future__ import annotations

import pytest
from pyspark.sql import DataFrame, SparkSession

from vet_lakehouse.dq import rules as R
from vet_lakehouse.dq.suite import has_blocking_failures


@pytest.fixture
def invoices(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(
        [
            ("INV-1", "CL001", 100.0, "2025-01-10"),
            ("INV-2", "CL001", -5.0, "2025-01-11"),
            ("INV-2", "CL002", 20.0, "1999-12-31"),
            ("INV-3", None, None, "2025-01-12"),
            ("INV-4", "CL999", 9000.0, "2025-01-13"),
        ],
        "invoice_id string, clinic_id string, amount double, invoice_date string",
    ).selectExpr("invoice_id", "clinic_id", "amount", "cast(invoice_date as date) invoice_date")


@pytest.fixture
def clinics(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame([("CL001",), ("CL002",)], "clinic_id string")


def test_not_null(invoices: DataFrame) -> None:
    ok = R.check_not_null(invoices, "silver.invoices", "invoice_id")
    bad = R.check_not_null(invoices, "silver.invoices", "clinic_id")

    assert ok.passed and ok.failed_count == 0 and ok.total_count == 5
    assert bad.status == R.FAIL and bad.failed_count == 1
    assert bad.check_name == "not_null:silver.invoices.clinic_id"
    assert bad.rule_type == "not_null" and bad.severity == R.ERROR


def test_unique(invoices: DataFrame) -> None:
    single = R.check_unique(invoices, "silver.invoices", ["invoice_id"])
    combined = R.check_unique(invoices, "silver.invoices", ["invoice_id", "clinic_id"])

    assert single.status == R.FAIL and single.failed_count == 1
    assert combined.passed, "the combination of both columns is unique"


def test_range_for_numbers_ignores_nulls(invoices: DataFrame) -> None:
    lower = R.check_range(invoices, "silver.invoices", "amount", min_value=0)
    both = R.check_range(invoices, "silver.invoices", "amount", min_value=0, max_value=1000)
    wide = R.check_range(invoices, "silver.invoices", "amount", min_value=-10, max_value=10_000)

    assert lower.failed_count == 1
    assert both.failed_count == 2
    assert wide.passed, "NULL values are not range failures (use not_null for that)"


def test_range_for_dates(invoices: DataFrame) -> None:
    result = R.check_range(invoices, "silver.invoices", "invoice_date", min_value="2000-01-01")
    assert result.failed_count == 1


def test_referential_integrity(invoices: DataFrame, clinics: DataFrame) -> None:
    result = R.check_referential_integrity(
        invoices, "silver.invoices", "clinic_id", clinics, "silver.clinics", "clinic_id"
    )
    assert result.status == R.FAIL
    assert result.failed_count == 1, "only CL999 is an orphan; NULL keys are not checked here"
    assert result.total_count == 4

    all_known = R.check_referential_integrity(
        invoices.filter("clinic_id <> 'CL999'"), "silver.invoices", "clinic_id",
        clinics, "silver.clinics", "clinic_id",
    )  # fmt: skip
    assert all_known.passed


def test_reconciliation() -> None:
    same = R.check_reconciliation("rows", "silver", 100, "gold", 100)
    rounding = R.check_reconciliation("total", "silver", 100.004, "gold", 100.0, tolerance=0.01)
    different = R.check_reconciliation("rows", "silver", 100, "gold", 98)
    empty = R.check_reconciliation("total", "silver", None, "gold", 0)

    assert same.passed and rounding.passed and empty.passed
    assert different.status == R.FAIL
    assert "difference=2.00" in different.details
    assert different.rule_type == "reconciliation"


def test_only_error_severity_blocks_the_pipeline(invoices: DataFrame) -> None:
    warning = R.check_not_null(invoices, "silver.invoices", "clinic_id", severity=R.WARN)
    error = R.check_not_null(invoices, "silver.invoices", "clinic_id", severity=R.ERROR)
    passed = R.check_not_null(invoices, "silver.invoices", "invoice_id")

    assert not has_blocking_failures([warning, passed])
    assert has_blocking_failures([warning, error, passed])


def test_result_as_dict_matches_the_results_table() -> None:
    result = R.check_reconciliation("rows", "silver", 1, "gold", 1)
    assert set(result.as_dict()) == {
        "check_name", "rule_type", "table_name", "column_name", "status", "severity",
        "failed_count", "total_count", "details",
    }  # fmt: skip
