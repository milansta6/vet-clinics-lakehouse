"""Bronze layer: raw, append-only Delta tables with ingestion metadata."""

from vet_lakehouse.bronze.ingest import run_bronze

__all__ = ["run_bronze"]
