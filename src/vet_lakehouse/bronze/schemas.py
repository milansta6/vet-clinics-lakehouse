"""Bronze sources and their schemas.

Bronze keeps the data as raw as possible: every scalar value is read as a
string, so nothing is lost or rejected at this stage. Nested JSON keeps its
structure (structs, arrays, maps). Typing and cleaning happen in Silver.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    MapType,
    StringType,
    StructField,
    StructType,
)


def _strings(*names: str) -> StructType:
    """Build a schema where every column is a nullable string."""
    return StructType([StructField(name, StringType(), True) for name in names])


CLINICS_SCHEMA = _strings(
    "clinic_id", "clinic_name", "city", "country_code", "country", "currency",
    "opened_date", "num_vets",
)  # fmt: skip

APPOINTMENTS_SCHEMA = _strings(
    "appointment_id", "clinic_id", "pet_id", "appointment_ts", "reason", "vet_name",
    "status", "duration_min", "updated_at",
)  # fmt: skip

INVOICES_SCHEMA = _strings(
    "invoice_id", "appointment_id", "clinic_id", "pet_id", "invoice_date", "amount",
    "currency", "payment_method", "status", "updated_at",
)  # fmt: skip

MANUAL_FX_SCHEMA = _strings("currency", "valid_from", "rate_per_eur", "source")

OWNERS_SCHEMA = StructType(
    [
        StructField("owner_id", StringType()),
        StructField("first_name", StringType()),
        StructField("last_name", StringType()),
        StructField("contact", _strings("email", "phone")),
        StructField("address", _strings("street", "city", "postal_code", "country_code")),
        StructField("registered_at", StringType()),
        StructField("marketing_consent", StringType()),
        StructField("updated_at", StringType()),
    ]
)

PETS_SCHEMA = StructType(
    [
        StructField("pet_id", StringType()),
        StructField("name", StringType()),
        StructField("species", StringType()),
        StructField("breed", StringType()),
        StructField("sex", StringType()),
        StructField("birth_date", StringType()),
        StructField("owner_id", StringType()),
        StructField("home_clinic_id", StringType()),
        StructField("microchip", _strings("number", "implanted_on")),
        StructField("weight_kg", StringType()),
        StructField("vaccinations", ArrayType(_strings("vaccine", "date"))),
        StructField("updated_at", StringType()),
    ]
)

# Raw Frankfurter API answer: {"base": "EUR", "rates": {"2025-01-02": {"SEK": 11.42}}}
FX_RATES_SCHEMA = StructType(
    [
        StructField("amount", DoubleType()),
        StructField("base", StringType()),
        StructField("start_date", StringType()),
        StructField("end_date", StringType()),
        StructField("rates", MapType(StringType(), MapType(StringType(), DoubleType()))),
    ]
)

CSV_OPTIONS = {"header": "true", "mode": "PERMISSIVE"}


@dataclass(frozen=True)
class BronzeSource:
    """One raw source: where its files land and how to read them.

    Attributes:
        name: Bronze table name (also the landing sub-folder).
        file_glob: Pattern of the files to pick up inside the sub-folder.
        file_format: Spark reader format (``csv`` or ``json``).
        schema: Explicit schema, so a bad file can never change the table schema.
        options: Reader options.
        partition_by_ingest_date: Partition the Bronze table by ``_ingest_date``.
            Used for the daily, ever-growing sources.
    """

    name: str
    file_glob: str
    file_format: str
    schema: StructType
    options: dict[str, str] = field(default_factory=dict)
    partition_by_ingest_date: bool = False


BRONZE_SOURCES: tuple[BronzeSource, ...] = (
    BronzeSource("clinics", "*.csv", "csv", CLINICS_SCHEMA, CSV_OPTIONS),
    BronzeSource("owners", "*.json", "json", OWNERS_SCHEMA),
    BronzeSource("pets", "*.json", "json", PETS_SCHEMA),
    BronzeSource("fx_rates", "*.json", "json", FX_RATES_SCHEMA, {"multiLine": "true"}),
    BronzeSource("fx_rates_manual", "*.csv", "csv", MANUAL_FX_SCHEMA, CSV_OPTIONS),
    BronzeSource(
        "appointments", "*.csv", "csv", APPOINTMENTS_SCHEMA, CSV_OPTIONS,
        partition_by_ingest_date=True,
    ),
    BronzeSource(
        "invoices", "*.csv", "csv", INVOICES_SCHEMA, CSV_OPTIONS,
        partition_by_ingest_date=True,
    ),
)  # fmt: skip
