# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze: raw ingestion
# MAGIC
# MAGIC * Reads only the files that are **not yet loaded** (incremental).
# MAGIC * Appends them unchanged to Delta tables (append-only, all values kept as raw strings).
# MAGIC * Adds the metadata columns `_ingested_at`, `_source_file` and `_batch_id`.

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

# COMMAND ----------

from vet_lakehouse.bronze import run_bronze
from vet_lakehouse.config import LakehouseConfig
from vet_lakehouse.spark import apply_session_defaults

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema_prefix", "vet", "Schema prefix")

cfg = LakehouseConfig.for_databricks(
    dbutils.widgets.get("catalog"), dbutils.widgets.get("schema_prefix")
)
apply_session_defaults(spark)

# COMMAND ----------

summary = run_bronze(spark, cfg)
display(
    spark.createDataFrame(
        [(name, s["new_files"], s["rows"]) for name, s in summary.items()],
        "source string, new_files int, rows_appended int",
    )
)

# COMMAND ----------

# MAGIC %md ## A look at the result

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT _batch_id, _ingested_at, COUNT(DISTINCT _source_file) AS files, COUNT(*) AS row_count
        FROM {cfg.table("bronze", "invoices")}
        GROUP BY _batch_id, _ingested_at
        ORDER BY _ingested_at
        """
    )
)

# COMMAND ----------

display(spark.table(cfg.table("bronze", "pets")).limit(5))
