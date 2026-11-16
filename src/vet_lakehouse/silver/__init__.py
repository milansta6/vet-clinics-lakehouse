"""Silver layer: cleaned, typed, deduplicated tables plus a quarantine table."""

from vet_lakehouse.silver.load import run_silver

__all__ = ["run_silver"]
