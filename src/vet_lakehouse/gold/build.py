"""Gold layer: a star schema and ready-to-use aggregates for Power BI.

Star schema
    ``fact_invoice`` (one row per invoice) with ``dim_date``, ``dim_clinic`` and ``dim_pet``.

Aggregates
    ``daily_revenue``, ``pets_treated_monthly`` and ``avg_invoice_value_monthly``.

The ``build_*`` functions are pure (DataFrame in, DataFrame out). ``run_gold``
rebuilds all Gold tables from Silver. A full rebuild is the simplest correct
choice at this size; ``docs/performance-and-cost.md`` explains when to switch
to incremental refresh.

Surrogate keys are a hash of the business key (``xxhash64``). They are stable
between rebuilds, so Power BI relationships and bookmarks keep working.
"""

from __future__ import annotations

import logging

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from vet_lakehouse.config import LakehouseConfig
from vet_lakehouse.tables import ensure_schemas, overwrite

logger = logging.getLogger(__name__)

UNKNOWN_KEY = -1  # dimension member used when a fact points to a missing dimension row
REVENUE_STATUSES = ("paid", "pending")  # cancelled invoices are not revenue


def surrogate_key(column: str) -> Column:
    """Return a stable 64-bit surrogate key for a business key column."""
    return F.xxhash64(F.col(column))


def date_key(column: str) -> Column:
    """Return the integer date key ``yyyyMMdd`` for a date column."""
    return F.date_format(F.col(column), "yyyyMMdd").cast("int")


# --------------------------------------------------------------------------- #
# Dimensions
# --------------------------------------------------------------------------- #
def build_dim_date(dates: DataFrame, date_column: str) -> DataFrame:
    """Build a gap-free calendar that covers every month found in ``dates``."""
    bounds = dates.agg(
        F.trunc(F.min(date_column), "month").alias("_first"),
        F.last_day(F.max(date_column)).alias("_last"),
    ).filter(F.col("_first").isNotNull())
    calendar = bounds.select(F.explode(F.sequence("_first", "_last")).alias("date"))
    iso_day = ((F.dayofweek("date") + 5) % 7) + 1  # Monday = 1 ... Sunday = 7
    return calendar.select(
        date_key("date").alias("date_key"),
        "date",
        F.year("date").alias("year"),
        F.quarter("date").alias("quarter"),
        F.month("date").alias("month"),
        F.date_format("date", "MMMM").alias("month_name"),
        F.date_format("date", "yyyy-MM").alias("year_month"),
        F.weekofyear("date").alias("iso_week"),
        iso_day.alias("day_of_week"),
        F.date_format("date", "EEEE").alias("day_name"),
        (iso_day >= 6).alias("is_weekend"),
    )


def build_dim_clinic(clinics: DataFrame) -> DataFrame:
    """Build the clinic dimension (type 1: always the current values)."""
    return clinics.select(
        surrogate_key("clinic_id").alias("clinic_key"),
        "clinic_id",
        "clinic_name",
        "city",
        "country_code",
        "country",
        "currency",
        "opened_date",
        "num_vets",
    )


def build_dim_pet(pets: DataFrame, owners: DataFrame) -> DataFrame:
    """Build the pet dimension, plus an "Unknown" member with key -1.

    Only the owner's country is taken from the owner record: no names, emails
    or addresses reach the Gold layer.
    """
    owner_country = owners.select(
        F.col("owner_id").alias("_owner_id"), F.col("country_code").alias("owner_country_code")
    )
    known = pets.join(owner_country, F.col("owner_id") == F.col("_owner_id"), "left").select(
        surrogate_key("pet_id").alias("pet_key"),
        "pet_id",
        F.col("name").alias("pet_name"),
        "species",
        "breed",
        "sex",
        "birth_date",
        "home_clinic_id",
        "owner_country_code",
        "is_microchipped",
        "vaccination_count",
        "last_vaccination_date",
    )
    unknown = pets.sparkSession.createDataFrame(
        [(UNKNOWN_KEY, "UNKNOWN", "Unknown", "unknown", None, "unknown", None, None, None)],
        "pet_key long, pet_id string, pet_name string, species string, breed string, "
        "sex string, birth_date date, home_clinic_id string, owner_country_code string",
    ).select(
        "*",
        F.lit(False).alias("is_microchipped"),
        F.lit(0).alias("vaccination_count"),
        F.lit(None).cast("date").alias("last_vaccination_date"),
    )
    return known.unionByName(unknown)


# --------------------------------------------------------------------------- #
# Fact
# --------------------------------------------------------------------------- #
def build_fact_invoice(invoices: DataFrame, dim_pet: DataFrame) -> DataFrame:
    """Build the invoice fact table (grain: one row per invoice).

    Invoices whose pet is not in ``dim_pet`` point to the "Unknown" member (-1)
    instead of being dropped, so revenue totals stay complete.
    """
    known_pets = dim_pet.select(
        F.col("pet_id").alias("_pet_id"), F.col("pet_key").alias("_pet_key")
    )
    return invoices.join(known_pets, F.col("pet_id") == F.col("_pet_id"), "left").select(
        "invoice_id",
        date_key("invoice_date").alias("date_key"),
        surrogate_key("clinic_id").alias("clinic_key"),
        F.coalesce("_pet_key", F.lit(UNKNOWN_KEY)).alias("pet_key"),
        "appointment_id",
        "invoice_date",
        "status",
        "payment_method",
        "currency",
        "amount_local",
        "fx_rate",
        "amount_eur",
        F.date_format("invoice_date", "yyyy-MM").alias("invoice_year_month"),
    )


# --------------------------------------------------------------------------- #
# Aggregates
# --------------------------------------------------------------------------- #
def build_daily_revenue(fact_invoice: DataFrame, dim_clinic: DataFrame) -> DataFrame:
    """Revenue in EUR per day and clinic (cancelled invoices are left out)."""
    revenue = fact_invoice.filter(F.col("status").isin(*REVENUE_STATUSES))
    return (
        revenue.join(F.broadcast(dim_clinic), "clinic_key")
        .groupBy("invoice_date", "clinic_id", "clinic_name", "city", "country_code", "country")
        .agg(
            F.count("*").alias("invoice_count"),
            F.sum("amount_eur").cast("decimal(18,2)").alias("revenue_eur"),
            F.round(F.avg("amount_eur"), 2).cast("decimal(18,2)").alias("avg_invoice_value_eur"),
        )
    )


def build_avg_invoice_value_monthly(fact_invoice: DataFrame, dim_clinic: DataFrame) -> DataFrame:
    """Average invoice value in EUR per month, country and clinic."""
    revenue = fact_invoice.filter(F.col("status").isin(*REVENUE_STATUSES))
    return (
        revenue.join(F.broadcast(dim_clinic), "clinic_key")
        .groupBy(
            F.col("invoice_year_month").alias("year_month"),
            "country_code",
            "country",
            "clinic_id",
            "clinic_name",
        )
        .agg(
            F.count("*").alias("invoice_count"),
            F.sum("amount_eur").cast("decimal(18,2)").alias("revenue_eur"),
            F.round(F.avg("amount_eur"), 2).cast("decimal(18,2)").alias("avg_invoice_value_eur"),
            F.max("amount_eur").alias("max_invoice_value_eur"),
        )
    )


def build_pets_treated_monthly(appointments: DataFrame, clinics: DataFrame) -> DataFrame:
    """Number of different pets treated per month and clinic (completed visits only)."""
    completed = appointments.filter(F.col("status") == "completed")
    clinic_info = F.broadcast(clinics.select("clinic_id", "clinic_name", "country_code", "country"))
    return (
        completed.join(clinic_info, "clinic_id")
        .groupBy(
            F.date_format("appointment_date", "yyyy-MM").alias("year_month"),
            "clinic_id",
            "clinic_name",
            "country_code",
            "country",
        )
        .agg(
            F.countDistinct("pet_id").alias("pets_treated"),
            F.count("*").alias("completed_appointments"),
        )
    )


# --------------------------------------------------------------------------- #
# Loader
# --------------------------------------------------------------------------- #
def run_gold(spark: SparkSession, cfg: LakehouseConfig) -> dict[str, int]:
    """Rebuild every Gold table from Silver. Returns the row count per table."""
    ensure_schemas(spark, cfg)

    def silver(name: str) -> DataFrame:
        return spark.table(cfg.table("silver", name))

    def publish(name: str, df: DataFrame, partition_by: list[str] | None = None) -> DataFrame:
        table = cfg.table("gold", name)
        overwrite(df, table, partition_by)
        return spark.table(table)

    summary: dict[str, int] = {}
    dim_clinic = publish("dim_clinic", build_dim_clinic(silver("clinics")))
    dim_pet = publish("dim_pet", build_dim_pet(silver("pets"), silver("owners")))
    dim_date = publish("dim_date", build_dim_date(silver("invoices"), "invoice_date"))
    fact = publish(
        "fact_invoice",
        build_fact_invoice(silver("invoices"), dim_pet),
        partition_by=["invoice_year_month"],
    )
    daily = publish("daily_revenue", build_daily_revenue(fact, dim_clinic))
    average = publish(
        "avg_invoice_value_monthly", build_avg_invoice_value_monthly(fact, dim_clinic)
    )
    treated = publish(
        "pets_treated_monthly",
        build_pets_treated_monthly(silver("appointments"), silver("clinics")),
    )

    tables = {
        "dim_clinic": dim_clinic,
        "dim_pet": dim_pet,
        "dim_date": dim_date,
        "fact_invoice": fact,
        "daily_revenue": daily,
        "avg_invoice_value_monthly": average,
        "pets_treated_monthly": treated,
    }
    for name, df in tables.items():
        summary[name] = df.count()
        logger.info("gold.%-26s rows=%d", name, summary[name])
    return summary
