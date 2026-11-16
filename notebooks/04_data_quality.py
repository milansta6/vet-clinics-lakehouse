# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Data quality checks
# MAGIC
# MAGIC Runs the rule-based suite (`not_null`, `unique`, `range`, `referential_integrity`,
# MAGIC `reconciliation`) and appends the results to `<prefix>_dq.dq_results`.
# MAGIC
# MAGIC The task **fails** when a check with severity `error` fails, so a scheduled job (or
# MAGIC Azure Data Factory) notices the problem. Checks with severity `warn` are only reported.

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

# COMMAND ----------

from vet_lakehouse.config import LakehouseConfig
from vet_lakehouse.dq import has_blocking_failures, run_dq
from vet_lakehouse.spark import apply_session_defaults

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema_prefix", "vet", "Schema prefix")

cfg = LakehouseConfig.for_databricks(
    dbutils.widgets.get("catalog"), dbutils.widgets.get("schema_prefix")
)
apply_session_defaults(spark)

# COMMAND ----------

results = run_dq(spark, cfg)

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT status, severity, rule_type, check_name, failed_count, total_count, details
        FROM {cfg.table("dq", "dq_results")}
        WHERE run_at = (SELECT MAX(run_at) FROM {cfg.table("dq", "dq_results")})
        ORDER BY status, severity, check_name
        """
    )
)

# COMMAND ----------

if has_blocking_failures(results):
    failed = [r.check_name for r in results if not r.passed and r.severity == "error"]
    raise RuntimeError(f"Blocking data quality checks failed: {failed}")

print(f"All blocking checks passed ({len(results)} checks in total).")
