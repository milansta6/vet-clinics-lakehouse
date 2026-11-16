"""Spark session factory.

* On Databricks the session already exists, so we just return it.
* Locally we build a Delta-enabled session. With ``persistent_catalog=True`` table
  metadata is stored in a small embedded metastore (Derby), so tables survive
  between separate commands such as ``make bronze`` and ``make silver``.
"""

from __future__ import annotations

import contextlib
import os
from pathlib import Path

from pyspark.sql import SparkSession


def is_databricks() -> bool:
    """Return True when the code runs on a Databricks runtime (classic or serverless)."""
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def apply_session_defaults(spark: SparkSession) -> None:
    """Set session options the transformations rely on.

    Each option is set on its own and failures are ignored, because serverless
    compute only allows a short list of Spark settings.
    """
    options = {
        "spark.sql.session.timeZone": "UTC",
        # Strict, predictable date parsing: bad dates become NULL instead of errors.
        "spark.sql.legacy.timeParserPolicy": "CORRECTED",
    }
    for key, value in options.items():
        with contextlib.suppress(Exception):
            spark.conf.set(key, value)


def get_spark(
    app_name: str = "vet-clinics-lakehouse",
    local_root: str = ".lakehouse",
    persistent_catalog: bool = True,
) -> SparkSession:
    """Return a Spark session that can read and write Delta tables.

    Args:
        app_name: Name shown in the Spark UI.
        local_root: Folder for the local warehouse and metastore.
        persistent_catalog: Keep table metadata on disk between runs (local only).
            Tests switch this off to get a faster in-memory catalog.
    """
    if is_databricks():
        spark = SparkSession.builder.getOrCreate()
        apply_session_defaults(spark)
        return spark

    from delta import configure_spark_with_delta_pip  # local-only dependency

    root = Path(local_root).resolve()
    root.mkdir(parents=True, exist_ok=True)

    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("VET_SPARK_MASTER", "local[*]"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.warehouse.dir", (root / "warehouse").as_posix())
        # Small data: a handful of shuffle partitions is faster than the default 200.
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.memory", os.environ.get("VET_DRIVER_MEMORY", "2g"))
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={root.as_posix()}")
    )
    if persistent_catalog:
        metastore = (root / "metastore_db").as_posix()
        builder = builder.config("spark.sql.catalogImplementation", "hive").config(
            "spark.hadoop.javax.jdo.option.ConnectionURL",
            f"jdbc:derby:;databaseName={metastore};create=true",
        )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    _silence_hive_alter_handler(spark)
    apply_session_defaults(spark)
    return spark


def _silence_hive_alter_handler(spark: SparkSession) -> None:
    """Hide one harmless log line of the local embedded metastore.

    When a Delta table is overwritten, Spark first tries to save its metadata
    in a Hive-compatible format, logs "Failed to alter table", and then falls
    back to its own format. The write itself succeeds. All other errors stay visible.
    """
    with contextlib.suppress(Exception):
        log4j = spark.sparkContext._jvm.org.apache.logging.log4j
        log4j.core.config.Configurator.setLevel(
            "org.apache.hadoop.hive.metastore.HiveAlterHandler", log4j.Level.OFF
        )
