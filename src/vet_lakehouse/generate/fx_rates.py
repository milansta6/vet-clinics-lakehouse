"""Exchange rates for converting invoices to EUR.

Two sources, like in a real company:

* **Frankfurter API** (https://api.frankfurter.app): free, real daily reference
  rates from the European Central Bank. The raw JSON answer is saved as-is.
  If the API cannot be reached (offline, CI, restricted network) a cached sample
  file from the repo is used instead.
* **Manual rates**: the ECB does not publish a rate for the Serbian dinar (RSD),
  so the Frankfurter API has no RSD. We pretend the finance team delivers a
  weekly CSV for it. These RSD numbers are synthetic.
"""

from __future__ import annotations

import csv
import json
import logging
import math
import shutil
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import requests

logger = logging.getLogger(__name__)

FRANKFURTER_URL = "https://api.frankfurter.app"
API_SYMBOLS: tuple[str, ...] = ("SEK",)
# repo_root/data/sample/fx_rates_sample.json (covers 2024-12-20 .. 2025-04-30)
DEFAULT_SAMPLE_PATH = (
    Path(__file__).resolve().parents[3] / "data" / "sample" / "fx_rates_sample.json"
)
LOOKBACK_DAYS = 7  # ask for a few earlier days so the first day always has a rate


def fetch_fx_rates(
    start: date, end: date, symbols: tuple[str, ...] = API_SYMBOLS, timeout: float = 15.0
) -> dict[str, Any]:
    """Download daily EUR reference rates from the Frankfurter API.

    Returns the parsed JSON document, for example::

        {"base": "EUR", "start_date": "...", "end_date": "...",
         "rates": {"2025-01-02": {"SEK": 11.4223}, ...}}

    Raises:
        requests.RequestException: The API cannot be reached or returns an error.
        ValueError: The answer does not contain any rates.
    """
    url = f"{FRANKFURTER_URL}/{start.isoformat()}..{end.isoformat()}"
    response = requests.get(url, params={"from": "EUR", "to": ",".join(symbols)}, timeout=timeout)
    response.raise_for_status()
    payload = response.json()
    if not payload.get("rates"):
        raise ValueError(f"Frankfurter API returned no rates for {url}")
    return payload


def write_fx_rates(
    landing: Path | str,
    start: date,
    end: date,
    live: bool = True,
    sample_path: Path | str = DEFAULT_SAMPLE_PATH,
) -> str:
    """Save raw FX rates as JSON in ``<landing>/fx_rates``.

    Args:
        landing: Landing folder.
        start: First business day that needs a rate.
        end: Last business day that needs a rate.
        live: Try the real API first. With ``False`` the cached sample is used.
        sample_path: Cached API answer used as the offline fallback.

    Returns:
        ``"live"`` or ``"sample"``, depending on which source was written.
    """
    target_dir = Path(landing) / "fx_rates"
    target_dir.mkdir(parents=True, exist_ok=True)

    if live:
        try:
            payload = fetch_fx_rates(start - timedelta(days=LOOKBACK_DAYS), end)
            target = target_dir / f"fx_rates_{start.isoformat()}_{end.isoformat()}.json"
            target.write_text(json.dumps(payload), encoding="utf-8")
            logger.info("FX rates downloaded from the Frankfurter API -> %s", target.name)
            return "live"
        except (requests.RequestException, ValueError) as error:
            logger.warning("Frankfurter API not available (%s). Using the cached sample.", error)

    shutil.copyfile(sample_path, target_dir / "fx_rates_sample.json")
    logger.info("FX rates copied from the cached sample file.")
    return "sample"


def build_manual_rates(start: date, end: date) -> list[dict[str, str]]:
    """Build the synthetic weekly RSD rates (one row per Monday)."""
    first = start - timedelta(days=LOOKBACK_DAYS)
    monday = first - timedelta(days=first.weekday())
    rows = []
    while monday <= end:
        week = monday.toordinal() // 7
        rows.append(
            {
                "currency": "RSD",
                "valid_from": monday.isoformat(),
                "rate_per_eur": f"{117.0 + 0.25 * math.sin(week):.4f}",
                "source": "finance_manual_upload",
            }
        )
        monday += timedelta(days=7)
    return rows


def write_manual_fx_rates(landing: Path | str, start: date, end: date) -> int:
    """Write the manual RSD rate file to ``<landing>/fx_rates_manual``. Returns the row count.

    The date range is part of the file name, so a later delivery that covers
    more weeks is a new file and gets picked up by the incremental load.
    """
    rows = build_manual_rates(start, end)
    name = f"manual_fx_rates_{start.isoformat()}_{end.isoformat()}.csv"
    target = Path(landing) / "fx_rates_manual" / name
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    return len(rows)
