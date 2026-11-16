"""Data quality: reusable rules and the suite that checks this lakehouse."""

from vet_lakehouse.dq.rules import DQResult
from vet_lakehouse.dq.suite import has_blocking_failures, run_dq

__all__ = ["DQResult", "has_blocking_failures", "run_dq"]
