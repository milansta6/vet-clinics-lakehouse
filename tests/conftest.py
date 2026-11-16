"""Shared pytest fixtures: one local Spark session and an isolated set of schemas per test."""

from __future__ import annotations

import uuid
from collections.abc import Callable, Iterator
from typing import Any

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from vet_lakehouse.config import LAYERS, LakehouseConfig
from vet_lakehouse.spark import get_spark
from vet_lakehouse.tables import ensure_schemas

DEFAULT_META = {
    "_source_file": "file:/landing/source/file_1.csv",
    "_batch_id": "batch-1",
    "_ingested_at": "2026-01-15 08:00:00",
}


@pytest.fixture(scope="session")
def spark(tmp_path_factory: pytest.TempPathFactory) -> Iterator[SparkSession]:
    """A local Delta-enabled Spark session shared by all tests."""
    root = tmp_path_factory.mktemp("lakehouse")
    session = get_spark("vet-lakehouse-tests", str(root), persistent_catalog=False)
    yield session
    session.stop()


@pytest.fixture
def cfg(spark: SparkSession, tmp_path) -> Iterator[LakehouseConfig]:
    """A config with its own schemas and landing folder, removed after the test."""
    config = LakehouseConfig(
        schema_prefix=f"t_{uuid.uuid4().hex[:8]}",
        landing_path=str(tmp_path / "landing"),
        local_root=str(tmp_path),
    )
    ensure_schemas(spark, config)
    yield config
    for layer in LAYERS:
        spark.sql(f"DROP SCHEMA IF EXISTS {config.schema(layer)} CASCADE")


@pytest.fixture
def bronze_df(spark: SparkSession) -> Callable[..., DataFrame]:
    """Factory for small Bronze-like DataFrames.

    Pass the business schema and rows as dicts. Missing columns become NULL and
    the ingestion metadata columns are added (they can be overridden per row).
    """

    def factory(schema: StructType, rows: list[dict[str, Any]]) -> DataFrame:
        full_schema = StructType(
            list(schema.fields) + [StructField(name, StringType()) for name in DEFAULT_META]
        )
        names = [f.name for f in full_schema.fields]
        data = [tuple({**DEFAULT_META, **row}.get(name) for name in names) for row in rows]
        return spark.createDataFrame(data, full_schema).withColumn(
            "_ingested_at", F.col("_ingested_at").cast("timestamp")
        )

    return factory
