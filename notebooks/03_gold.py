# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Gold: star schema and aggregates for Power BI
# MAGIC
# MAGIC * Star schema: `fact_invoice` + `dim_date`, `dim_clinic`, `dim_pet`
# MAGIC * Aggregates: `daily_revenue`, `pets_treated_monthly`, `avg_invoice_value_monthly`

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

# COMMAND ----------

from vet_lakehouse.config import LakehouseConfig
from vet_lakehouse.gold import run_gold
from vet_lakehouse.spark import apply_session_defaults

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema_prefix", "vet", "Schema prefix")

cfg = LakehouseConfig.for_databricks(
    dbutils.widgets.get("catalog"), dbutils.widgets.get("schema_prefix")
)
apply_session_defaults(spark)

# COMMAND ----------

summary = run_gold(spark, cfg)
display(spark.createDataFrame(list(summary.items()), "gold_table string, row_count int"))

# COMMAND ----------

# MAGIC %md ## Daily revenue per country

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT invoice_date, country, SUM(revenue_eur) AS revenue_eur
        FROM {cfg.table("gold", "daily_revenue")}
        GROUP BY invoice_date, country
        ORDER BY invoice_date, country
        """
    )
)

# COMMAND ----------

# MAGIC %md ## Star schema query (the same joins Power BI would do)

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT d.year_month, c.country, p.species,
               COUNT(*)          AS invoices,
               SUM(f.amount_eur) AS revenue_eur
        FROM {cfg.table("gold", "fact_invoice")} f
        JOIN {cfg.table("gold", "dim_date")}   d ON d.date_key   = f.date_key
        JOIN {cfg.table("gold", "dim_clinic")} c ON c.clinic_key = f.clinic_key
        JOIN {cfg.table("gold", "dim_pet")}    p ON p.pet_key    = f.pet_key
        WHERE f.status <> 'cancelled'
        GROUP BY d.year_month, c.country, p.species
        ORDER BY d.year_month, revenue_eur DESC
        """
    )
)
