# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Setup and source data
# MAGIC
# MAGIC 1. Creates the schemas (`<prefix>_bronze`, `_silver`, `_gold`, `_dq`) and a Unity Catalog
# MAGIC    **volume** that plays the role of the landing zone.
# MAGIC 2. Generates the synthetic source files into the volume.
# MAGIC 3. Downloads real EUR exchange rates from the Frankfurter API. If the workspace has no
# MAGIC    internet access (common on Free Edition), the cached sample file from the repo is used.
# MAGIC
# MAGIC In a real project this notebook would not exist: source systems would drop files into
# MAGIC ADLS Gen2 and the pipeline would start at Bronze.

# COMMAND ----------

import os
import sys

# Make the shared package (src/vet_lakehouse) importable from the bundle files.
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

# COMMAND ----------

from datetime import date

from vet_lakehouse.config import LakehouseConfig
from vet_lakehouse.generate import generate_all
from vet_lakehouse.tables import ensure_schemas

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema_prefix", "vet", "Schema prefix")
dbutils.widgets.text("start_date", "2025-01-01", "First business day")
dbutils.widgets.text("days", "59", "Number of daily files")
dbutils.widgets.dropdown("live_fx", "true", ["true", "false"], "Call the Frankfurter API")

cfg = LakehouseConfig.for_databricks(
    dbutils.widgets.get("catalog"), dbutils.widgets.get("schema_prefix")
)
print(cfg)

# COMMAND ----------

# MAGIC %md ## Schemas and landing volume

# COMMAND ----------

ensure_schemas(spark, cfg)
spark.sql(f"CREATE VOLUME IF NOT EXISTS {cfg.schema('bronze')}.landing")
display(spark.sql(f"SHOW SCHEMAS IN {cfg.catalog} LIKE '{cfg.schema_prefix}_*'"))

# COMMAND ----------

# MAGIC %md ## Generate source files
# MAGIC The generator is deterministic. Running it again with more `days` only adds new daily
# MAGIC files, which is how the incremental load can be shown: run the job, raise `days`, run again.

# COMMAND ----------

manifest = generate_all(
    landing=cfg.landing_path,
    start=date.fromisoformat(dbutils.widgets.get("start_date")),
    days=int(dbutils.widgets.get("days")),
    live_fx=dbutils.widgets.get("live_fx") == "true",
)
print(f"FX rates source: {manifest['fx_source']}")
display(
    spark.createDataFrame(
        [
            (name, s["files"], s["rows"], s["hard_dq_rows"])
            for name, s in manifest["sources"].items()
        ],
        "source string, files int, rows int, rows_with_hard_dq_problem int",
    )
)

# COMMAND ----------

display(dbutils.fs.ls(f"{cfg.landing_path}/invoices")[:10])
