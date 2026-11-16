"""End-to-end test: generate -> bronze -> silver -> gold -> dq on a small data set.

It checks the promises the README makes: bad rows are quarantined (exactly the
ones the generator planted), layers reconcile, and a second run only processes
new files.
"""

from __future__ import annotations

from datetime import date

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from vet_lakehouse.bronze import run_bronze
from vet_lakehouse.config import LakehouseConfig
from vet_lakehouse.dq import has_blocking_failures, run_dq
from vet_lakehouse.generate import generate_all
from vet_lakehouse.gold import run_gold
from vet_lakehouse.maintenance import run_optimize
from vet_lakehouse.silver import run_silver

START = date(2025, 1, 1)
pytestmark = pytest.mark.e2e


def _run_all(spark: SparkSession, cfg: LakehouseConfig, days: int) -> tuple[dict, dict, list]:
    manifest = generate_all(cfg.landing_path, START, days=days, live_fx=False)
    bronze = run_bronze(spark, cfg)
    run_silver(spark, cfg)
    run_gold(spark, cfg)
    results = run_dq(spark, cfg)
    return manifest, bronze, results


def _count(spark: SparkSession, table: str, condition: str = "true") -> int:
    return spark.table(table).filter(condition).count()


def test_full_pipeline_and_incremental_run(spark: SparkSession, cfg: LakehouseConfig) -> None:
    # ---------------- first run: 12 days ----------------
    manifest, _, results = _run_all(spark, cfg, days=12)
    quarantine = cfg.table("silver", "quarantine")

    failed = [r for r in results if not r.passed and r.severity == "error"]
    assert not has_blocking_failures(results), [f"{r.check_name}: {r.details}" for r in failed]

    # Silver quarantines exactly the rows the generator planted as "hard" problems.
    for source, stats in manifest["sources"].items():
        quarantined = _count(spark, quarantine, f"source_table = '{source}'")
        assert quarantined == stats["hard_dq_rows"], source

    # Silver has one row per business key and only clean values.
    invoices = spark.table(cfg.table("silver", "invoices"))
    assert invoices.count() == invoices.select("invoice_id").distinct().count()
    assert (
        invoices.filter("amount_local < 0 or amount_eur is null or clinic_id = 'CL999'").count()
        == 0
    )
    assert {r["currency"] for r in invoices.select("currency").distinct().collect()} == {
        "EUR", "SEK", "RSD",
    }  # fmt: skip
    statuses = {r["status"] for r in invoices.select("status").distinct().collect()}
    assert statuses <= {"paid", "pending", "cancelled"}, "status spellings are standardised"

    # Converted amounts are plausible: SEK and RSD amounts are much larger than their EUR value.
    ratios = {
        r["currency"]: r["ratio"]
        for r in invoices.groupBy("currency")
        .agg((F.sum("amount_local") / F.sum("amount_eur")).cast("double").alias("ratio"))
        .collect()
    }
    assert ratios["EUR"] == pytest.approx(1.0)
    assert 10 < ratios["SEK"] < 13
    assert 110 < ratios["RSD"] < 125

    # Gold matches Silver.
    fact = spark.table(cfg.table("gold", "fact_invoice"))
    assert fact.count() == invoices.count()
    silver_revenue = invoices.filter("status <> 'cancelled'").agg(F.sum("amount_eur")).first()[0]
    gold_revenue = (
        spark.table(cfg.table("gold", "daily_revenue")).agg(F.sum("revenue_eur")).first()[0]
    )
    assert gold_revenue == silver_revenue
    assert _count(spark, cfg.table("gold", "dim_clinic")) == 14
    assert _count(spark, cfg.table("gold", "dim_date")) == 31
    assert _count(spark, cfg.table("gold", "pets_treated_monthly")) > 0

    # DQ results are stored.
    dq_table = cfg.table("dq", "dq_results")
    assert _count(spark, dq_table) == len(results)
    rule_types = {r["rule_type"] for r in spark.table(dq_table).select("rule_type").collect()}
    assert rule_types == {"not_null", "unique", "range", "referential_integrity", "reconciliation"}

    counts_before = {
        "bronze": _count(spark, cfg.table("bronze", "invoices")),
        "silver": invoices.count(),
        "quarantine": _count(spark, quarantine),
        "pending": _count(spark, cfg.table("silver", "invoices"), "status = 'pending'"),
    }

    # ---------------- re-run without new files: nothing changes ----------------
    bronze_again = run_bronze(spark, cfg)
    silver_again = run_silver(spark, cfg)
    assert all(s["new_files"] == 0 for s in bronze_again.values())
    assert all(s["new_bronze_rows"] == 0 for s in silver_again.values())
    assert _count(spark, cfg.table("silver", "invoices")) == counts_before["silver"]
    assert _count(spark, quarantine) == counts_before["quarantine"]

    # ---------------- second run: 4 more days ----------------
    manifest_2, bronze_2, results_2 = _run_all(spark, cfg, days=16)
    assert bronze_2["invoices"]["new_files"] == 4
    assert bronze_2["appointments"]["new_files"] == 4
    assert bronze_2["clinics"]["new_files"] == 0
    assert not has_blocking_failures(results_2)

    assert _count(spark, cfg.table("bronze", "invoices")) > counts_before["bronze"]
    invoices = spark.table(cfg.table("silver", "invoices"))
    assert invoices.count() > counts_before["silver"]
    assert invoices.count() == invoices.select("invoice_id").distinct().count()
    for source, stats in manifest_2["sources"].items():
        quarantined = _count(spark, quarantine, f"source_table = '{source}'")
        assert quarantined == stats["hard_dq_rows"], source

    # MERGE updated existing rows: day-10..12 invoices that were pending are now paid.
    updated = invoices.filter(
        "status = 'paid' and invoice_date <= '2025-01-12' and date(updated_at) > '2025-01-12'"
    ).count()
    assert updated > 0, "expected pending invoices from the first run to be updated to paid"

    # Maintenance runs without errors.
    assert len(run_optimize(spark, cfg)) > 0
    assert _count(spark, dq_table) == len(results) + len(results_2)
