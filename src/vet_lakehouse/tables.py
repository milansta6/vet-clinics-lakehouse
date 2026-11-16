"""Small helpers for writing Delta tables: append, overwrite and MERGE (upsert).

Only SQL and DataFrame APIs that work on both open-source Spark and Databricks
serverless (Spark Connect) are used here.
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence

from pyspark.sql import DataFrame, SparkSession

from vet_lakehouse.config import LAYERS, LakehouseConfig


def ensure_schemas(spark: SparkSession, cfg: LakehouseConfig) -> None:
    """Create the bronze, silver, gold and dq schemas if they do not exist."""
    for layer in LAYERS:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.schema(layer)}")


def table_exists(spark: SparkSession, table: str) -> bool:
    """Return True if the three-level table name exists."""
    return bool(spark.catalog.tableExists(table))


def append(df: DataFrame, table: str, partition_by: Sequence[str] | None = None) -> None:
    """Append rows to a Delta table (the table is created on first write)."""
    writer = df.write.format("delta").mode("append")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.saveAsTable(table)


def overwrite(df: DataFrame, table: str, partition_by: Sequence[str] | None = None) -> None:
    """Replace the full content of a Delta table."""
    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.saveAsTable(table)


def merge_upsert(
    source: DataFrame,
    target: str,
    keys: Sequence[str],
    newer_than_column: str | None = None,
    partition_by: Sequence[str] | None = None,
) -> None:
    """Upsert ``source`` into ``target`` with a Delta MERGE.

    Rows that match on ``keys`` are updated, new rows are inserted. Running the
    same input twice gives the same result (the operation is idempotent).

    Args:
        source: New or changed rows. Must hold at most one row per key.
        target: Three-level name of the Delta table.
        keys: Business key columns used to match rows.
        newer_than_column: If set, a matched row is only updated when the source
            value of this column is greater than or equal to the target value.
            This stops late-arriving old data from overwriting newer data.
        partition_by: Partition columns, used only when the table is first created.
    """
    spark = source.sparkSession
    if not table_exists(spark, target):
        append(source, target, partition_by)
        return

    view = f"_merge_source_{uuid.uuid4().hex}"
    source.createOrReplaceTempView(view)
    on_clause = " AND ".join(f"t.`{k}` <=> s.`{k}`" for k in keys)
    matched_condition = (
        f"AND s.`{newer_than_column}` >= t.`{newer_than_column}`" if newer_than_column else ""
    )
    try:
        spark.sql(
            f"""
            MERGE INTO {target} AS t
            USING {view} AS s
            ON {on_clause}
            WHEN MATCHED {matched_condition} THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """
        )
    finally:
        spark.catalog.dropTempView(view)
