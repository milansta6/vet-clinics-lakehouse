"""Command line entry point for the local run.

Examples::

    python -m vet_lakehouse.pipeline run --reset        # generate data + all layers
    python -m vet_lakehouse.pipeline run --days 73      # add two more weeks (incremental)
    python -m vet_lakehouse.pipeline silver             # run one stage only
"""

from __future__ import annotations

import argparse
import logging
import shutil
import sys
import uuid
from collections.abc import Sequence
from datetime import date
from pathlib import Path

from pyspark.sql import SparkSession

from vet_lakehouse.bronze import run_bronze
from vet_lakehouse.config import LAYERS, LakehouseConfig
from vet_lakehouse.dq import has_blocking_failures, run_dq
from vet_lakehouse.generate import DEFAULT_DAYS, DEFAULT_START_DATE, generate_all
from vet_lakehouse.gold import run_gold
from vet_lakehouse.maintenance import run_optimize
from vet_lakehouse.silver import run_silver
from vet_lakehouse.spark import get_spark

logger = logging.getLogger("vet_lakehouse")

STAGES = ("generate", "bronze", "silver", "gold", "dq", "optimize", "summary", "run")


def print_summary(spark: SparkSession, cfg: LakehouseConfig) -> None:
    """Print the row count of every table and a small sample of the Gold layer."""
    print("\n=== Tables ===")
    for layer in LAYERS:
        schema = cfg.schema(layer)
        for row in spark.sql(f"SHOW TABLES IN {schema}").collect():
            if row["isTemporary"]:
                continue
            table = f"{schema}.{row['tableName']}"
            print(f"{table:<55} {spark.table(table).count():>8} rows")

    print("\n=== gold.daily_revenue: revenue per country (EUR) ===")
    spark.sql(
        f"""
        SELECT country,
               SUM(invoice_count)                           AS invoices,
               SUM(revenue_eur)                             AS revenue_eur,
               ROUND(SUM(revenue_eur) / SUM(invoice_count), 2) AS avg_invoice_eur
        FROM {cfg.table("gold", "daily_revenue")}
        GROUP BY country
        ORDER BY revenue_eur DESC
        """
    ).show(truncate=False)

    print("=== silver.quarantine: rejected rows per source and rule ===")
    spark.sql(
        f"""
        SELECT source_table, rule AS failed_rule, COUNT(*) AS row_count
        FROM {cfg.table("silver", "quarantine")}
        LATERAL VIEW explode(failed_rules) r AS rule
        GROUP BY source_table, rule
        ORDER BY source_table, row_count DESC
        """
    ).show(50, truncate=False)

    print("=== dq.dq_results: latest run ===")
    spark.sql(
        f"""
        SELECT rule_type, severity, status, COUNT(*) AS checks
        FROM {cfg.table("dq", "dq_results")}
        WHERE run_at = (SELECT MAX(run_at) FROM {cfg.table("dq", "dq_results")})
        GROUP BY rule_type, severity, status
        ORDER BY rule_type, severity, status
        """
    ).show(truncate=False)


def build_parser() -> argparse.ArgumentParser:
    """Build the command line parser."""
    parser = argparse.ArgumentParser(
        prog="vet_lakehouse.pipeline", description="Run the vet clinics lakehouse locally."
    )
    parser.add_argument("stage", choices=STAGES, help="Stage to run. 'run' = everything.")
    parser.add_argument(
        "--start-date",
        type=date.fromisoformat,
        default=DEFAULT_START_DATE,
        help="First business day of the synthetic data (default: %(default)s).",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_DAYS,
        help="Number of daily files to generate (default: %(default)s).",
    )
    parser.add_argument("--seed", type=int, default=42, help="Random seed of the generator.")
    parser.add_argument(
        "--offline",
        action="store_true",
        help="Do not call the Frankfurter API; use the cached FX sample file.",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Delete the local lakehouse folder first (fresh start).",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the requested stage. Returns the process exit code."""
    args = build_parser().parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s")
    cfg = LakehouseConfig.from_env()

    if args.reset and Path(cfg.local_root).exists():
        root = Path(cfg.local_root).resolve()
        if root in (Path.cwd().resolve(), Path.home().resolve(), Path(root.anchor)):
            raise SystemExit(f"Refusing to delete {root}. Point VET_LOCAL_ROOT to its own folder.")
        logger.info("Removing local lakehouse folder %s", root)
        shutil.rmtree(root)

    stage = args.stage
    if stage in ("generate", "run"):
        generate_all(cfg.landing_path, args.start_date, args.days, args.seed, not args.offline)
        if stage == "generate":
            return 0

    spark = get_spark(local_root=cfg.local_root)
    exit_code = 0
    try:
        batch_id = uuid.uuid4().hex[:12]
        if stage in ("bronze", "run"):
            run_bronze(spark, cfg, batch_id)
        if stage in ("silver", "run"):
            run_silver(spark, cfg)
        if stage in ("gold", "run"):
            run_gold(spark, cfg)
        if stage in ("dq", "run"):
            results = run_dq(spark, cfg, batch_id)
            if has_blocking_failures(results):
                logger.error("Blocking data quality checks failed.")
                exit_code = 1
        if stage == "optimize":
            run_optimize(spark, cfg)
        if stage in ("summary", "run"):
            print_summary(spark, cfg)
    finally:
        spark.stop()
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
