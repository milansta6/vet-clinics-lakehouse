# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver: clean, typed, deduplicated
# MAGIC
# MAGIC For every entity:
# MAGIC 1. take only the Bronze rows that arrived since the last run (watermark),
# MAGIC 2. clean and type them, convert invoice amounts to EUR,
# MAGIC 3. send rows that break a rule to `quarantine` (with the rule names and the raw record),
# MAGIC 4. keep the latest version per business key and **MERGE** it into the Silver table.

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

# COMMAND ----------

from vet_lakehouse.config import LakehouseConfig
from vet_lakehouse.silver import run_silver
from vet_lakehouse.spark import apply_session_defaults

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema_prefix", "vet", "Schema prefix")

cfg = LakehouseConfig.for_databricks(
    dbutils.widgets.get("catalog"), dbutils.widgets.get("schema_prefix")
)
apply_session_defaults(spark)

# COMMAND ----------

summary = run_silver(spark, cfg)
display(
    spark.createDataFrame(
        [(name, s["new_bronze_rows"], s["quarantined"]) for name, s in summary.items()],
        "entity string, new_bronze_rows int, quarantined int",
    )
)

# COMMAND ----------

# MAGIC %md ## Quarantine: what was rejected and why

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT source_table, rule AS failed_rule, COUNT(*) AS row_count
        FROM {cfg.table("silver", "quarantine")}
        LATERAL VIEW explode(failed_rules) r AS rule
        GROUP BY source_table, rule
        ORDER BY source_table, row_count DESC
        """
    )
)

# COMMAND ----------

display(spark.table(cfg.table("silver", "quarantine")).limit(10))

# COMMAND ----------

# MAGIC %md ## Currency conversion check

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT currency, COUNT(*) AS invoices,
               ROUND(AVG(fx_rate), 4) AS avg_fx_rate,
               SUM(amount_local) AS amount_local, SUM(amount_eur) AS amount_eur
        FROM {cfg.table("silver", "invoices")}
        GROUP BY currency
        """
    )
)
