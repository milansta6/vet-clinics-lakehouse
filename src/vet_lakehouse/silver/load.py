"""Silver layer loader: incremental MERGE from Bronze, with a quarantine table.

For every entity the flow is the same:

1. Read only the Bronze rows that arrived after the last successful run
   (a watermark on ``_ingested_at`` is stored per Silver table).
2. Clean, type and check the rows (see ``transforms.py``).
3. Rows that break a rule go to ``quarantine`` with the reason and the raw record.
4. Valid rows are deduplicated (latest version per key) and merged into Silver.
5. The watermark moves forward.

Every step can be repeated safely: MERGE is idempotent and quarantine rows of
a batch are replaced, not added twice.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from vet_lakehouse.config import LakehouseConfig
from vet_lakehouse.silver import transforms as T
from vet_lakehouse.tables import append, ensure_schemas, merge_upsert, table_exists

logger = logging.getLogger(__name__)

WATERMARK_TABLE = "pipeline_watermarks"
QUARANTINE_TABLE = "quarantine"
_TS_PATTERN = "yyyy-MM-dd HH:mm:ss.SSSSSS"


@dataclass(frozen=True)
class SilverEntity:
    """How one Bronze table becomes one Silver table.

    Attributes:
        name: Table name, the same in Bronze and Silver.
        keys: Business key used for deduplication and MERGE.
        transform: Function ``(spark, cfg, new_bronze_rows) -> checked rows``.
    """

    name: str
    keys: tuple[str, ...]
    transform: Callable[[SparkSession, LakehouseConfig, DataFrame], DataFrame]


def _silver(spark: SparkSession, cfg: LakehouseConfig, name: str) -> DataFrame:
    return spark.table(cfg.table("silver", name))


ENTITIES: tuple[SilverEntity, ...] = (
    # Order matters: clinics and FX rates are needed by appointments and invoices.
    SilverEntity("clinics", ("clinic_id",), lambda spark, cfg, df: T.transform_clinics(df)),
    SilverEntity("owners", ("owner_id",), lambda spark, cfg, df: T.transform_owners(df)),
    SilverEntity("pets", ("pet_id",), lambda spark, cfg, df: T.transform_pets(df)),
    SilverEntity(
        "appointments",
        ("appointment_id",),
        lambda spark, cfg, df: T.transform_appointments(df, _silver(spark, cfg, "clinics")),
    ),
    SilverEntity(
        "invoices",
        ("invoice_id",),
        lambda spark, cfg, df: T.transform_invoices(
            df, _silver(spark, cfg, "clinics"), _silver(spark, cfg, "fx_rates")
        ),
    ),
)


# --------------------------------------------------------------------------- #
# Watermarks
# --------------------------------------------------------------------------- #
def get_watermark(spark: SparkSession, cfg: LakehouseConfig, name: str) -> str | None:
    """Return the last processed ``_ingested_at`` of a Silver table (as text), or None."""
    table = cfg.table("silver", WATERMARK_TABLE)
    if not table_exists(spark, table):
        return None
    rows = (
        spark.table(table)
        .filter(F.col("target_table") == name)
        .select(F.date_format("last_ingested_at", _TS_PATTERN).alias("value"))
        .collect()
    )
    return rows[0]["value"] if rows else None


def set_watermark(spark: SparkSession, cfg: LakehouseConfig, name: str, value: str) -> None:
    """Store the new watermark of a Silver table."""
    row = spark.createDataFrame([(name, value)], "target_table string, value string").select(
        "target_table",
        F.col("value").cast("timestamp").alias("last_ingested_at"),
        F.current_timestamp().alias("updated_at"),
    )
    merge_upsert(row, cfg.table("silver", WATERMARK_TABLE), keys=["target_table"])


def read_new_bronze(
    spark: SparkSession, cfg: LakehouseConfig, name: str
) -> tuple[DataFrame, str | None]:
    """Return the Bronze rows not yet processed into Silver, and their newest ``_ingested_at``."""
    bronze = spark.table(cfg.table("bronze", name))
    watermark = get_watermark(spark, cfg, name)
    if watermark is not None:
        bronze = bronze.filter(F.col("_ingested_at") > F.lit(watermark).cast("timestamp"))
    newest = bronze.agg(F.date_format(F.max("_ingested_at"), _TS_PATTERN).alias("value")).collect()[
        0
    ]["value"]
    return bronze, newest


# --------------------------------------------------------------------------- #
# Quarantine
# --------------------------------------------------------------------------- #
def write_quarantine(
    spark: SparkSession,
    cfg: LakehouseConfig,
    rejected: DataFrame,
    source_table: str,
    key_column: str,
    batch_ids: Sequence[str],
) -> None:
    """Write rejected rows to the quarantine table.

    Existing quarantine rows of the same source and batches are deleted first,
    so re-running a batch does not create duplicates.
    """
    table = cfg.table("silver", QUARANTINE_TABLE)
    if table_exists(spark, table) and batch_ids:
        batches = ", ".join(f"'{b}'" for b in batch_ids)
        spark.sql(
            f"DELETE FROM {table} "
            f"WHERE source_table = '{source_table}' AND _batch_id IN ({batches})"
        )
    append(T.to_quarantine(rejected, source_table, key_column), table)


# --------------------------------------------------------------------------- #
# Loaders
# --------------------------------------------------------------------------- #
def load_entity(spark: SparkSession, cfg: LakehouseConfig, entity: SilverEntity) -> dict[str, int]:
    """Process the new Bronze rows of one entity into Silver and quarantine."""
    new_rows, newest = read_new_bronze(spark, cfg, entity.name)
    if newest is None:
        return {"new_bronze_rows": 0, "quarantined": 0}

    checked = entity.transform(spark, cfg, new_rows)
    valid, rejected = T.split_valid_rejected(checked)
    latest = T.deduplicate_latest(valid, entity.keys).withColumn(
        "_silver_updated_at", F.current_timestamp()
    )
    merge_upsert(
        latest, cfg.table("silver", entity.name), entity.keys, newer_than_column="_ingested_at"
    )

    batch_ids = [r["_batch_id"] for r in new_rows.select("_batch_id").distinct().collect()]
    write_quarantine(spark, cfg, rejected, entity.name, entity.keys[0], batch_ids)
    set_watermark(spark, cfg, entity.name, newest)

    quarantined = (
        spark.table(cfg.table("silver", QUARANTINE_TABLE))
        .filter((F.col("source_table") == entity.name) & F.col("_batch_id").isin(batch_ids))
        .count()
    )
    return {"new_bronze_rows": new_rows.count(), "quarantined": quarantined}


def load_fx_rates(spark: SparkSession, cfg: LakehouseConfig) -> int:
    """Rebuild the FX rates from all Bronze rate rows and merge them into Silver.

    The table is tiny and forward-filling needs the full history, so this is a
    full recompute followed by a MERGE on ``(rate_date, currency)``.
    """
    rates = T.build_fx_rates(
        spark.table(cfg.table("bronze", "fx_rates")),
        spark.table(cfg.table("bronze", "fx_rates_manual")),
    ).withColumn("_silver_updated_at", F.current_timestamp())
    table = cfg.table("silver", "fx_rates")
    merge_upsert(rates, table, keys=["rate_date", "currency"])
    return spark.table(table).count()


def run_silver(spark: SparkSession, cfg: LakehouseConfig) -> dict[str, dict[str, int]]:
    """Run the whole Silver layer.

    Returns:
        Per entity, the number of new Bronze rows processed and rows quarantined.
    """
    ensure_schemas(spark, cfg)
    summary: dict[str, dict[str, int]] = {}

    fx_rows = load_fx_rates(spark, cfg)
    logger.info("silver.%-13s rows_total=%d", "fx_rates", fx_rows)

    for entity in ENTITIES:
        stats = load_entity(spark, cfg, entity)
        summary[entity.name] = stats
        silver_rows = (
            spark.table(cfg.table("silver", entity.name)).count()
            if table_exists(spark, cfg.table("silver", entity.name))
            else 0
        )
        logger.info(
            "silver.%-13s new_bronze_rows=%-5d quarantined=%-4d rows_total=%d",
            entity.name,
            stats["new_bronze_rows"],
            stats["quarantined"],
            silver_rows,
        )
    return summary
