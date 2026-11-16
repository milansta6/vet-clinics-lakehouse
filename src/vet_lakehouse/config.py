"""Central configuration: where tables live and where raw files land.

All table names are three-level names (``catalog.schema.table``), the same style
Unity Catalog uses. Locally the catalog is Spark's built-in ``spark_catalog``;
on Databricks it is a Unity Catalog catalog such as ``workspace``.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

LAYERS = ("bronze", "silver", "gold", "dq")


@dataclass(frozen=True)
class LakehouseConfig:
    """Settings shared by every pipeline stage.

    Attributes:
        catalog: Catalog name (first level of the table name).
        schema_prefix: Prefix for the four schemas, e.g. ``vet`` gives
            ``vet_bronze``, ``vet_silver``, ``vet_gold`` and ``vet_dq``.
        landing_path: Folder where raw source files arrive.
        local_root: Folder for the local warehouse and metastore (local runs only).
    """

    catalog: str = "spark_catalog"
    schema_prefix: str = "vet"
    landing_path: str = ".lakehouse/landing"
    local_root: str = ".lakehouse"

    def schema(self, layer: str) -> str:
        """Return the two-level schema name for a layer, e.g. ``spark_catalog.vet_silver``."""
        if layer not in LAYERS:
            raise ValueError(f"Unknown layer '{layer}'. Expected one of {LAYERS}.")
        return f"{self.catalog}.{self.schema_prefix}_{layer}"

    def table(self, layer: str, name: str) -> str:
        """Return the three-level table name, e.g. ``spark_catalog.vet_silver.invoices``."""
        return f"{self.schema(layer)}.{name}"

    @classmethod
    def from_env(cls) -> LakehouseConfig:
        """Build the config from ``VET_*`` environment variables (used by the local CLI)."""
        local_root = os.environ.get("VET_LOCAL_ROOT", ".lakehouse")
        return cls(
            catalog=os.environ.get("VET_CATALOG", "spark_catalog"),
            schema_prefix=os.environ.get("VET_SCHEMA_PREFIX", "vet"),
            landing_path=os.environ.get("VET_LANDING_PATH", f"{local_root}/landing"),
            local_root=local_root,
        )

    @classmethod
    def for_databricks(cls, catalog: str, schema_prefix: str) -> LakehouseConfig:
        """Build the config for Databricks: raw files land in a Unity Catalog volume."""
        return cls(
            catalog=catalog,
            schema_prefix=schema_prefix,
            landing_path=f"/Volumes/{catalog}/{schema_prefix}_bronze/landing",
        )
