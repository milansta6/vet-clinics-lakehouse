"""The data quality suite of this lakehouse.

``build_results`` runs all checks, ``run_dq`` also stores them in the
``dq_results`` Delta table (one row per check and run), so quality can be
followed over time in a dashboard.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from vet_lakehouse.config import LakehouseConfig
from vet_lakehouse.dq import rules as R
from vet_lakehouse.dq.rules import DQResult
from vet_lakehouse.tables import append, ensure_schemas

logger = logging.getLogger(__name__)

RESULTS_TABLE = "dq_results"
MAX_QUARANTINE_SHARE = 0.10  # warn when more than 10% of a source is quarantined
MONEY_TOLERANCE = 0.01

# Bronze source -> business key column
RECONCILED_SOURCES = {
    "clinics": "clinic_id",
    "owners": "owner_id",
    "pets": "pet_id",
    "appointments": "appointment_id",
    "invoices": "invoice_id",
}

RESULTS_SCHEMA = (
    "run_id string, run_at string, check_name string, rule_type string, table_name string, "
    "column_name string, status string, severity string, failed_count long, total_count long, "
    "details string"
)


def _scalar(df: DataFrame, expression: str) -> float:
    """Return one aggregated number from a DataFrame (0 for NULL)."""
    value = df.agg(F.expr(expression).alias("value")).collect()[0]["value"]
    return float(value or 0)


def _clean_key(column: str) -> Column:
    """Normalise a raw Bronze key the same way Silver does (trim, empty -> NULL)."""
    return F.nullif(F.trim(F.col(column)), F.lit(""))


def silver_checks(spark: SparkSession, cfg: LakehouseConfig) -> list[DQResult]:
    """not_null, unique, range and referential integrity checks on Silver."""

    def silver(name: str) -> DataFrame:
        return spark.table(cfg.table("silver", name))

    clinics, owners, pets = silver("clinics"), silver("owners"), silver("pets")
    appointments, invoices, fx_rates = (
        silver("appointments"),
        silver("invoices"),
        silver("fx_rates"),
    )

    results = [
        R.check_unique(clinics, "silver.clinics", ["clinic_id"]),
        R.check_unique(owners, "silver.owners", ["owner_id"]),
        R.check_unique(pets, "silver.pets", ["pet_id"]),
        R.check_unique(appointments, "silver.appointments", ["appointment_id"]),
        R.check_unique(invoices, "silver.invoices", ["invoice_id"]),
        R.check_unique(fx_rates, "silver.fx_rates", ["rate_date", "currency"]),
    ]
    for column in ("invoice_id", "clinic_id", "invoice_date", "amount_local", "amount_eur"):
        results.append(R.check_not_null(invoices, "silver.invoices", column))
    for column in ("appointment_id", "clinic_id", "pet_id", "appointment_ts"):
        results.append(R.check_not_null(appointments, "silver.appointments", column))
    results += [
        R.check_not_null(pets, "silver.pets", "species", severity=R.WARN),
        R.check_not_null(owners, "silver.owners", "email", severity=R.WARN),
        R.check_range(invoices, "silver.invoices", "amount_local", min_value=0),
        R.check_range(invoices, "silver.invoices", "amount_eur", min_value=0, max_value=50_000),
        R.check_range(invoices, "silver.invoices", "invoice_date", min_value="2000-01-01"),
        R.check_range(fx_rates, "silver.fx_rates", "rate_per_eur", min_value=0.0001),
        R.check_range(pets, "silver.pets", "weight_kg", min_value=0, max_value=150),
        R.check_range(
            appointments, "silver.appointments", "duration_min", min_value=1, max_value=480,
            severity=R.WARN,
        ),
        R.check_referential_integrity(
            invoices, "silver.invoices", "clinic_id", clinics, "silver.clinics", "clinic_id"
        ),
        R.check_referential_integrity(
            appointments, "silver.appointments", "clinic_id", clinics, "silver.clinics", "clinic_id"
        ),
        R.check_referential_integrity(
            invoices, "silver.invoices", "pet_id", pets, "silver.pets", "pet_id", severity=R.WARN
        ),
        R.check_referential_integrity(
            pets, "silver.pets", "owner_id", owners, "silver.owners", "owner_id", severity=R.WARN
        ),
    ]  # fmt: skip
    return results


def gold_checks(spark: SparkSession, cfg: LakehouseConfig) -> list[DQResult]:
    """Key and relationship checks on the Gold star schema."""

    def gold(name: str) -> DataFrame:
        return spark.table(cfg.table("gold", name))

    fact = gold("fact_invoice")
    dim_date, dim_clinic, dim_pet = gold("dim_date"), gold("dim_clinic"), gold("dim_pet")
    return [
        R.check_unique(fact, "gold.fact_invoice", ["invoice_id"]),
        R.check_unique(dim_date, "gold.dim_date", ["date_key"]),
        R.check_unique(dim_clinic, "gold.dim_clinic", ["clinic_key"]),
        R.check_unique(dim_pet, "gold.dim_pet", ["pet_key"]),
        R.check_unique(gold("daily_revenue"), "gold.daily_revenue", ["invoice_date", "clinic_id"]),
        R.check_not_null(fact, "gold.fact_invoice", "amount_eur"),
        R.check_referential_integrity(
            fact, "gold.fact_invoice", "date_key", dim_date, "gold.dim_date", "date_key"
        ),
        R.check_referential_integrity(
            fact, "gold.fact_invoice", "clinic_key", dim_clinic, "gold.dim_clinic", "clinic_key"
        ),
        R.check_referential_integrity(
            fact, "gold.fact_invoice", "pet_key", dim_pet, "gold.dim_pet", "pet_key"
        ),
    ]


def reconciliation_checks(spark: SparkSession, cfg: LakehouseConfig) -> list[DQResult]:
    """Compare row counts and money totals between Bronze, Silver and Gold.

    Bronze -> Silver
        Every business key that arrived in Bronze must be in Silver or in
        quarantine (nothing is silently lost), and every Bronze row without a
        key must be in quarantine.
    Silver -> Gold
        Row counts and EUR totals must be identical.
    """
    quarantine = spark.table(cfg.table("silver", "quarantine"))
    results: list[DQResult] = []

    for source, key in RECONCILED_SOURCES.items():
        bronze = spark.table(cfg.table("bronze", source)).select(_clean_key(key).alias("key"))
        silver = spark.table(cfg.table("silver", source)).select(F.col(key).alias("key"))
        rejected = quarantine.filter(F.col("source_table") == source)

        bronze_keys = _scalar(bronze, "count(distinct key)")
        accounted = silver.union(rejected.select(F.col("record_key").alias("key")))
        results.append(
            R.check_reconciliation(
                f"distinct_keys:{source}",
                f"bronze.{source}",
                bronze_keys,
                "silver+quarantine",
                _scalar(accounted, "count(distinct key)"),
            )  # fmt: skip
        )
        results.append(
            R.check_reconciliation(
                f"rows_without_key:{source}",
                f"bronze.{source}",
                _scalar(bronze, "count(case when key is null then 1 end)"),
                "quarantine",
                _scalar(rejected, "count(case when record_key is null then 1 end)"),
            )  # fmt: skip
        )
        bronze_rows = _scalar(bronze, "count(*)")
        share = _scalar(rejected, "count(*)") / bronze_rows if bronze_rows else 0.0
        results.append(
            DQResult(
                check_name=f"quarantine_share:{source}",
                rule_type="range",
                table_name="silver.quarantine",
                column_name=None,
                status=R.PASS if share <= MAX_QUARANTINE_SHARE else R.FAIL,
                severity=R.WARN,
                failed_count=int(_scalar(rejected, "count(*)")),
                total_count=int(bronze_rows),
                details=f"{share:.2%} of bronze.{source} rows are quarantined "
                f"(limit {MAX_QUARANTINE_SHARE:.0%})",
            )
        )

    invoices = spark.table(cfg.table("silver", "invoices"))
    fact = spark.table(cfg.table("gold", "fact_invoice"))
    daily = spark.table(cfg.table("gold", "daily_revenue"))
    monthly = spark.table(cfg.table("gold", "avg_invoice_value_monthly"))
    revenue_filter = "status in ('paid', 'pending')"
    results += [
        R.check_reconciliation(
            "row_count:invoices",
            "silver.invoices", _scalar(invoices, "count(*)"),
            "gold.fact_invoice", _scalar(fact, "count(*)"),
        ),
        R.check_reconciliation(
            "total_eur:invoices",
            "silver.invoices", _scalar(invoices, "sum(amount_eur)"),
            "gold.fact_invoice", _scalar(fact, "sum(amount_eur)"),
            tolerance=MONEY_TOLERANCE,
        ),
        R.check_reconciliation(
            "revenue_eur:daily_revenue",
            "gold.fact_invoice", _scalar(fact.filter(revenue_filter), "sum(amount_eur)"),
            "gold.daily_revenue", _scalar(daily, "sum(revenue_eur)"),
            tolerance=MONEY_TOLERANCE,
        ),
        R.check_reconciliation(
            "invoice_count:daily_revenue",
            "gold.fact_invoice", _scalar(fact.filter(revenue_filter), "count(*)"),
            "gold.daily_revenue", _scalar(daily, "sum(invoice_count)"),
        ),
        R.check_reconciliation(
            "revenue_eur:avg_invoice_value_monthly",
            "gold.daily_revenue", _scalar(daily, "sum(revenue_eur)"),
            "gold.avg_invoice_value_monthly", _scalar(monthly, "sum(revenue_eur)"),
            tolerance=MONEY_TOLERANCE,
        ),
    ]  # fmt: skip
    return results


def build_results(spark: SparkSession, cfg: LakehouseConfig) -> list[DQResult]:
    """Run every check of the suite and return the results."""
    return silver_checks(spark, cfg) + gold_checks(spark, cfg) + reconciliation_checks(spark, cfg)


def write_results(
    spark: SparkSession, cfg: LakehouseConfig, results: list[DQResult], run_id: str
) -> None:
    """Append the results of one run to the ``dq_results`` table."""
    run_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    rows = [{"run_id": run_id, "run_at": run_at, **result.as_dict()} for result in results]
    df = spark.createDataFrame(rows, RESULTS_SCHEMA).withColumn(
        "run_at", F.col("run_at").cast("timestamp")
    )
    append(df, cfg.table("dq", RESULTS_TABLE))


def has_blocking_failures(results: list[DQResult]) -> bool:
    """True when at least one check with severity ``error`` failed."""
    return any(not r.passed and r.severity == R.ERROR for r in results)


def run_dq(spark: SparkSession, cfg: LakehouseConfig, run_id: str | None = None) -> list[DQResult]:
    """Run the suite, store the results and log a short summary."""
    ensure_schemas(spark, cfg)
    run_id = run_id or uuid.uuid4().hex[:12]
    results = build_results(spark, cfg)
    write_results(spark, cfg, results, run_id)

    failed = [r for r in results if not r.passed]
    logger.info(
        "dq: %d checks, %d passed, %d failed (%d blocking) -> %s",
        len(results),
        len(results) - len(failed),
        len(failed),
        sum(1 for r in failed if r.severity == R.ERROR),
        cfg.table("dq", RESULTS_TABLE),
    )
    for result in failed:
        logger.warning(
            "dq %s [%s] %s: %s",
            result.status, result.severity, result.check_name, result.details,
        )  # fmt: skip
    return results
