# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Maintenance (optional, not part of the daily job)
# MAGIC
# MAGIC * `OPTIMIZE` merges many small files into fewer large ones (faster reads, fewer API calls).
# MAGIC * `ZORDER BY` sorts the data so queries that filter on those columns can skip files.
# MAGIC * `VACUUM` removes old files that are no longer part of the table (lower storage cost).
# MAGIC
# MAGIC Run this weekly, not after every load. More background: `docs/performance-and-cost.md`.

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

# COMMAND ----------

from vet_lakehouse.config import LakehouseConfig
from vet_lakehouse.maintenance import run_optimize

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema_prefix", "vet", "Schema prefix")

cfg = LakehouseConfig.for_databricks(
    dbutils.widgets.get("catalog"), dbutils.widgets.get("schema_prefix")
)

# COMMAND ----------

for statement in run_optimize(spark, cfg):
    print(statement)

# COMMAND ----------

# MAGIC %md ## File layout before / after
# MAGIC `numFiles` should go down after OPTIMIZE.

# COMMAND ----------

display(
    spark.sql(f"DESCRIBE DETAIL {cfg.table('silver', 'invoices')}").select(
        "name", "numFiles", "sizeInBytes", "partitionColumns"
    )
)

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {cfg.table('silver', 'invoices')}").limit(10))

# COMMAND ----------

# MAGIC %md ## VACUUM
# MAGIC The default keeps 7 days of history (needed for time travel and for readers that are
# MAGIC still running). Do not lower it without a good reason.

# COMMAND ----------

spark.sql(f"VACUUM {cfg.table('silver', 'invoices')}")
