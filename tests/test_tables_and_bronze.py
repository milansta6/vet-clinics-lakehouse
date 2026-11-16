"""Tests for the Delta helpers (MERGE) and the incremental Bronze ingestion."""

from __future__ import annotations

from datetime import date
from pathlib import Path

from pyspark.sql import SparkSession

from vet_lakehouse.bronze.ingest import file_key, run_bronze
from vet_lakehouse.config import LakehouseConfig
from vet_lakehouse.generate import generate_all
from vet_lakehouse.tables import merge_upsert

START = date(2025, 1, 1)


def _people(spark: SparkSession, rows: list[tuple[str, str, int]]):
    return spark.createDataFrame(rows, "id string, name string, version int")


def test_merge_upsert_inserts_and_updates(spark: SparkSession, cfg: LakehouseConfig) -> None:
    table = cfg.table("silver", "people")
    merge_upsert(_people(spark, [("1", "Anna", 1), ("2", "Bram", 1)]), table, ["id"])
    merge_upsert(_people(spark, [("2", "Bram B.", 2), ("3", "Clara", 1)]), table, ["id"])

    rows = {r["id"]: r["name"] for r in spark.table(table).collect()}
    assert rows == {"1": "Anna", "2": "Bram B.", "3": "Clara"}


def test_merge_upsert_is_idempotent(spark: SparkSession, cfg: LakehouseConfig) -> None:
    table = cfg.table("silver", "people")
    batch = _people(spark, [("1", "Anna", 1), ("2", "Bram", 1)])
    merge_upsert(batch, table, ["id"])
    merge_upsert(batch, table, ["id"])
    assert spark.table(table).count() == 2


def test_merge_upsert_ignores_older_versions(spark: SparkSession, cfg: LakehouseConfig) -> None:
    table = cfg.table("silver", "people")
    merge_upsert(_people(spark, [("1", "Anna v5", 5)]), table, ["id"], newer_than_column="version")
    merge_upsert(_people(spark, [("1", "Anna v3", 3)]), table, ["id"], newer_than_column="version")
    assert spark.table(table).first()["name"] == "Anna v5"


def test_file_key_is_the_same_for_every_path_style() -> None:
    expected = "invoices/invoices_2025-01-01.csv"
    assert file_key("/Volumes/main/vet_bronze/landing/invoices/invoices_2025-01-01.csv") == expected
    assert file_key("file:///app/.lakehouse/landing/invoices/invoices_2025-01-01.csv") == expected
    assert file_key(r"C:\data\landing\invoices\invoices_2025-01-01.csv") == expected


def test_bronze_is_incremental_and_append_only(spark: SparkSession, cfg: LakehouseConfig) -> None:
    generate_all(cfg.landing_path, START, days=3, live_fx=False)

    first = run_bronze(spark, cfg, batch_id="batch-1")
    assert first["invoices"]["new_files"] == 3
    assert first["clinics"]["new_files"] == 1
    invoices = spark.table(cfg.table("bronze", "invoices"))
    rows_after_first_run = invoices.count()
    assert rows_after_first_run == first["invoices"]["rows"] > 0

    # Metadata columns are filled, and the raw values are still strings.
    row = invoices.first()
    assert row["_batch_id"] == "batch-1"
    assert row["_ingested_at"] is not None
    assert Path(row["_source_file"]).name.startswith("invoices_2025-01-0")
    assert dict(invoices.dtypes)["amount"] == "string"

    # Same files again: nothing is loaded.
    second = run_bronze(spark, cfg, batch_id="batch-2")
    assert all(stats["new_files"] == 0 for stats in second.values())
    assert invoices.count() == rows_after_first_run

    # Two more days arrive: only the new files are loaded, old rows are untouched.
    generate_all(cfg.landing_path, START, days=5, live_fx=False)
    third = run_bronze(spark, cfg, batch_id="batch-3")
    assert third["invoices"]["new_files"] == 2
    assert third["appointments"]["new_files"] == 2
    assert third["clinics"]["new_files"] == 0
    assert third["fx_rates"]["new_files"] == 0
    assert third["fx_rates_manual"]["new_files"] == 1, "the manual file covers a new date range"

    invoices = spark.table(cfg.table("bronze", "invoices"))
    assert invoices.count() == rows_after_first_run + third["invoices"]["rows"]
    assert invoices.filter("_batch_id = 'batch-1'").count() == rows_after_first_run
    assert invoices.select("_source_file").distinct().count() == 5


def test_bronze_keeps_nested_json(spark: SparkSession, cfg: LakehouseConfig) -> None:
    generate_all(cfg.landing_path, START, days=1, live_fx=False)
    run_bronze(spark, cfg)

    pets = dict(spark.table(cfg.table("bronze", "pets")).dtypes)
    assert pets["microchip"].startswith("struct<")
    assert pets["vaccinations"].startswith("array<struct<")
    fx = spark.table(cfg.table("bronze", "fx_rates"))
    assert dict(fx.dtypes)["rates"] == "map<string,map<string,double>>"
    assert fx.count() == 1, "one raw API document per file"
