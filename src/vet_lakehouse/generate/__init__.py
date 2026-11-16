"""Source data generation: synthetic clinic data plus real EUR exchange rates."""

from __future__ import annotations

import logging
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from vet_lakehouse.generate.fx_rates import write_fx_rates, write_manual_fx_rates
from vet_lakehouse.generate.synthetic import generate_landing_data

logger = logging.getLogger(__name__)

DEFAULT_START_DATE = date(2025, 1, 1)
DEFAULT_DAYS = 59  # January + February 2025


def generate_all(
    landing: Path | str,
    start: date = DEFAULT_START_DATE,
    days: int = DEFAULT_DAYS,
    seed: int = 42,
    live_fx: bool = True,
) -> dict[str, Any]:
    """Write every source file to the landing folder and return the manifest."""
    end = start + timedelta(days=days - 1)
    manifest = generate_landing_data(landing, start, days, seed)
    manifest["fx_source"] = write_fx_rates(landing, start, end, live=live_fx)
    manifest["manual_fx_rows"] = write_manual_fx_rates(landing, start, end)
    for source, stats in manifest["sources"].items():
        logger.info(
            "generated %-12s files=%-3d rows=%-5d rows_with_hard_dq_problem=%d",
            source,
            stats["files"],
            stats["rows"],
            stats["hard_dq_rows"],
        )
    return manifest


__all__ = ["DEFAULT_DAYS", "DEFAULT_START_DATE", "generate_all", "generate_landing_data"]
