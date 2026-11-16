"""Reusable, rule-based data quality checks.

Each check is a small function that looks at a DataFrame (or two numbers) and
returns a :class:`DQResult`. The checks know nothing about this project, so
they can be reused for any table.

Severity
    ``error``: a failed check should fail the pipeline run.
    ``warn``: a failed check is reported, but the run continues.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import asdict, dataclass
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

PASS = "PASS"
FAIL = "FAIL"
ERROR = "error"
WARN = "warn"


@dataclass(frozen=True)
class DQResult:
    """Outcome of one data quality check."""

    check_name: str
    rule_type: str
    table_name: str
    column_name: str | None
    status: str
    severity: str
    failed_count: int
    total_count: int
    details: str

    @property
    def passed(self) -> bool:
        """True when the check passed."""
        return self.status == PASS

    def as_dict(self) -> dict[str, Any]:
        """Return the result as a plain dict (one row of the ``dq_results`` table)."""
        return asdict(self)


def _result(
    rule_type: str,
    table_name: str,
    column_name: str | None,
    failed: int,
    total: int,
    severity: str,
    details: str,
) -> DQResult:
    column_part = f".{column_name}" if column_name else ""
    return DQResult(
        check_name=f"{rule_type}:{table_name}{column_part}",
        rule_type=rule_type,
        table_name=table_name,
        column_name=column_name,
        status=PASS if failed == 0 else FAIL,
        severity=severity,
        failed_count=int(failed),
        total_count=int(total),
        details=details,
    )


def check_not_null(df: DataFrame, table_name: str, column: str, severity: str = ERROR) -> DQResult:
    """Check that a column has no NULL values."""
    row = df.agg(
        F.count("*").alias("total"),
        F.count(F.when(F.col(column).isNull(), 1)).alias("failed"),
    ).collect()[0]
    return _result(
        "not_null", table_name, column, row["failed"], row["total"], severity,
        f"{row['failed']} of {row['total']} rows have NULL in {column}",
    )  # fmt: skip


def check_unique(
    df: DataFrame, table_name: str, columns: Sequence[str], severity: str = ERROR
) -> DQResult:
    """Check that a column (or a combination of columns) has no duplicate values."""
    row = df.agg(
        F.count("*").alias("total"),
        F.countDistinct(F.struct(*columns)).alias("distinct_keys"),
    ).collect()[0]
    duplicates = row["total"] - row["distinct_keys"]
    name = ",".join(columns)
    return _result(
        "unique", table_name, name, duplicates, row["total"], severity,
        f"{duplicates} duplicate rows for key ({name})",
    )  # fmt: skip


def check_range(
    df: DataFrame,
    table_name: str,
    column: str,
    min_value: float | str | None = None,
    max_value: float | str | None = None,
    severity: str = ERROR,
) -> DQResult:
    """Check that all non-NULL values of a column are inside ``[min_value, max_value]``.

    Works for numbers and for dates (pass dates as ``'yyyy-MM-dd'`` strings).
    """
    value = F.col(column)
    out_of_range = F.lit(False)
    if min_value is not None:
        out_of_range = out_of_range | (value < F.lit(min_value))
    if max_value is not None:
        out_of_range = out_of_range | (value > F.lit(max_value))
    row = df.agg(
        F.count("*").alias("total"),
        F.count(F.when(out_of_range, 1)).alias("failed"),
    ).collect()[0]
    return _result(
        "range", table_name, column, row["failed"], row["total"], severity,
        f"{row['failed']} rows outside [{min_value}, {max_value}]",
    )  # fmt: skip


def check_referential_integrity(
    child: DataFrame,
    child_table: str,
    child_column: str,
    parent: DataFrame,
    parent_table: str,
    parent_column: str,
    severity: str = ERROR,
) -> DQResult:
    """Check that every non-NULL child key exists in the parent table."""
    child_keys = child.filter(F.col(child_column).isNotNull()).select(
        F.col(child_column).alias("_key")
    )
    parent_keys = parent.select(F.col(parent_column).alias("_key")).distinct()
    total = child_keys.count()
    orphans = child_keys.join(parent_keys, "_key", "left_anti").count()
    return _result(
        "referential_integrity", child_table, child_column, orphans, total, severity,
        f"{orphans} rows point to a missing {parent_table}.{parent_column}",
    )  # fmt: skip


def check_reconciliation(
    name: str,
    left_label: str,
    left_value: float | int | None,
    right_label: str,
    right_value: float | int | None,
    tolerance: float = 0.0,
    severity: str = ERROR,
) -> DQResult:
    """Check that two numbers from different layers match (within a tolerance).

    Used for row counts and money totals, for example Silver invoices versus
    the Gold fact table.
    """
    left = float(left_value or 0)
    right = float(right_value or 0)
    difference = abs(left - right)
    return DQResult(
        check_name=f"reconciliation:{name}",
        rule_type="reconciliation",
        table_name=f"{left_label} vs {right_label}",
        column_name=None,
        status=PASS if difference <= tolerance else FAIL,
        severity=severity,
        failed_count=0 if difference <= tolerance else 1,
        total_count=1,
        details=f"{left_label}={left:.2f}, {right_label}={right:.2f}, difference={difference:.2f}",
    )
