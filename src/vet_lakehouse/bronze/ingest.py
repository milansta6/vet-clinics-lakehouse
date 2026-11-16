"""Bronze layer: incremental, append-only ingestion of raw files into Delta.

How "only new files" works
--------------------------
Every Bronze row stores the file it came from in ``_source_file``. Before a run
we list the landing folder and skip every file that is already in the table.
Because the list of loaded files lives in the same Delta table as the data,
a file is either fully loaded or not loaded at all (one atomic commit).

On a large production system the same idea is provided by Databricks Auto
Loader (``cloudFiles``), which tracks files in a checkpoint instead of scanning
the table. See ``docs/performance-and-cost.md``.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from vet_lakehouse.bronze.schemas import BRONZE_SOURCES, BronzeSource
from vet_lakehouse.config import LakehouseConfig
from vet_lakehouse.tables import append, ensure_schemas, table_exists

logger = logging.getLogger(__name__)

TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S.%f"


def file_key(path: str) -> str:
    """Return a stable id for a landed file: ``<source folder>/<file name>``.

    The same file can be addressed as ``/Volumes/...``, ``file:///...`` or
    ``dbfs:/...``. The last two path parts are the same in all of them.
    """
    return "/".join(path.replace("\\", "/").rstrip("/").split("/")[-2:])


def list_landing_files(landing_path: str, source: BronzeSource) -> list[Path]:
    """List all files of a source that are currently in the landing folder."""
    folder = Path(landing_path) / source.name
    if not folder.is_dir():
        return []
    return sorted(p for p in folder.glob(source.file_glob) if p.is_file())


def ingested_file_keys(spark: SparkSession, table: str) -> set[str]:
    """Return the keys of all files that were already loaded into a Bronze table."""
    if not table_exists(spark, table):
        return set()
    rows = spark.table(table).select("_source_file").distinct().collect()
    return {file_key(row["_source_file"]) for row in rows}


def ingest_source(
    spark: SparkSession,
    cfg: LakehouseConfig,
    source: BronzeSource,
    batch_id: str,
    ingested_at: str,
) -> dict[str, int]:
    """Append the new files of one source to its Bronze table.

    Args:
        spark: Active Spark session.
        cfg: Lakehouse configuration.
        source: The source to load.
        batch_id: Id of this pipeline run, stored in ``_batch_id``.
        ingested_at: Load time (UTC, ``yyyy-MM-dd HH:mm:ss.ffffff``), stored in
            ``_ingested_at``. One value for the whole batch.

    Returns:
        ``{"new_files": ..., "rows": ...}`` for logging and tests.
    """
    table = cfg.table("bronze", source.name)
    known = ingested_file_keys(spark, table)
    new_files = [
        path
        for path in list_landing_files(cfg.landing_path, source)
        if file_key(path.as_posix()) not in known
    ]
    if not new_files:
        return {"new_files": 0, "rows": 0}

    raw = (
        spark.read.format(source.file_format)
        .schema(source.schema)
        .options(**source.options)
        .load([path.resolve().as_posix() for path in new_files])
    )
    with_metadata = (
        raw.select("*", F.col("_metadata.file_path").alias("_source_file"))
        .withColumn("_ingested_at", F.lit(ingested_at).cast("timestamp"))
        .withColumn("_batch_id", F.lit(batch_id))
    )
    partition_by = None
    if source.partition_by_ingest_date:
        with_metadata = with_metadata.withColumn("_ingest_date", F.to_date("_ingested_at"))
        partition_by = ["_ingest_date"]

    append(with_metadata, table, partition_by)
    rows = spark.table(table).filter(F.col("_batch_id") == batch_id).count()
    return {"new_files": len(new_files), "rows": rows}


def run_bronze(
    spark: SparkSession, cfg: LakehouseConfig, batch_id: str | None = None
) -> dict[str, dict[str, int]]:
    """Load all new landing files into the Bronze tables.

    Returns:
        Per source, the number of new files and rows loaded in this run.
    """
    ensure_schemas(spark, cfg)
    batch_id = batch_id or uuid.uuid4().hex[:12]
    ingested_at = datetime.now(timezone.utc).strftime(TIMESTAMP_FORMAT)

    summary: dict[str, dict[str, int]] = {}
    for source in BRONZE_SOURCES:
        stats = ingest_source(spark, cfg, source, batch_id, ingested_at)
        summary[source.name] = stats
        logger.info(
            "bronze.%-16s new_files=%-3d rows_appended=%d",
            source.name,
            stats["new_files"],
            stats["rows"],
        )
    return summary
