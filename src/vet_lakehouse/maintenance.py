"""Table maintenance: file compaction (OPTIMIZE) and data clustering (Z-ORDER).

Run this on a schedule (for example weekly), not after every load: compaction
rewrites files and costs compute. See ``docs/performance-and-cost.md``.
"""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession

from vet_lakehouse.config import LakehouseConfig
from vet_lakehouse.tables import table_exists

logger = logging.getLogger(__name__)

# (layer, table, Z-ORDER columns). Z-ORDER columns are the ones used most in filters
# and joins. Partition columns cannot be used for Z-ORDER.
OPTIMIZE_TARGETS: tuple[tuple[str, str, tuple[str, ...]], ...] = (
    ("bronze", "appointments", ()),
    ("bronze", "invoices", ()),
    ("silver", "appointments", ("clinic_id", "appointment_date")),
    ("silver", "invoices", ("clinic_id", "invoice_date")),
    ("silver", "quarantine", ("source_table",)),
    ("gold", "fact_invoice", ("clinic_key", "date_key")),
    ("gold", "daily_revenue", ("invoice_date", "clinic_id")),
)


def run_optimize(spark: SparkSession, cfg: LakehouseConfig) -> list[str]:
    """Compact small files and cluster the data of the main tables.

    Returns:
        The SQL statements that were executed.
    """
    statements = []
    for layer, name, zorder_columns in OPTIMIZE_TARGETS:
        table = cfg.table(layer, name)
        if not table_exists(spark, table):
            continue
        statement = f"OPTIMIZE {table}"
        if zorder_columns:
            statement += f" ZORDER BY ({', '.join(zorder_columns)})"
        spark.sql(statement)
        statements.append(statement)
        logger.info(statement)
    return statements
