"""Silver transformations: pure functions from DataFrame to DataFrame.

Nothing in this module reads or writes tables, so every function is easy to
unit test with a few rows.

Each ``transform_*`` function returns the cleaned and typed rows **plus**:

* ``_dq_failures``: array with the names of the data quality rules a row breaks
  (empty array = valid row),
* ``_raw``: the original Bronze row as a JSON string, kept for the quarantine table.

Parsing never raises an error (also with ANSI mode on, the Databricks default):
values are first checked with a regular expression, and bad values become NULL.
"""

from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

META_COLUMNS = ["_source_file", "_batch_id", "_ingested_at"]
VALID_COUNTRY_CODES = ("BE", "NL", "DE", "SE", "RS")
APPOINTMENT_STATUSES = ("completed", "cancelled", "no_show")
INVOICE_STATUSES = ("paid", "pending", "cancelled")
MIN_BUSINESS_DATE = "2000-01-01"
MAX_PET_WEIGHT_KG = 150.0
MAX_FORWARD_FILL_DAYS = 7  # how long the last known FX rate may be reused

_ISO_DATE = r"^\d{4}-\d{2}-\d{2}$"
_EU_DATE = r"^\d{2}/\d{2}/\d{4}$"
_ISO_TS = r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$"
_EU_TS = r"^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$"
_INTEGER = r"^-?\d{1,9}$"
_DECIMAL = r"^-?\d{1,9}(\.\d+)?$"
_EMAIL = r"^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$"


# --------------------------------------------------------------------------- #
# Column helpers
# --------------------------------------------------------------------------- #
def clean_string(column: Column) -> Column:
    """Trim a string and turn empty strings into NULL."""
    return F.nullif(F.trim(column), F.lit(""))


def parse_date(column: Column) -> Column:
    """Parse ``yyyy-MM-dd`` or ``dd/MM/yyyy`` into a date. Anything else gives NULL."""
    text = F.trim(column)
    iso = F.when(text.rlike(_ISO_DATE), F.try_to_timestamp(text, F.lit("yyyy-MM-dd")))
    european = F.when(text.rlike(_EU_DATE), F.try_to_timestamp(text, F.lit("dd/MM/yyyy")))
    return F.coalesce(iso, european).cast("date")


def parse_timestamp(column: Column) -> Column:
    """Parse the timestamp formats found in the sources. Anything else gives NULL.

    Accepted: ``yyyy-MM-dd HH:mm:ss``, ``yyyy-MM-ddTHH:mm:ssZ`` and ``dd/MM/yyyy HH:mm:ss``.
    """
    text = F.regexp_replace(F.regexp_replace(F.trim(column), "T", " "), "Z$", "")
    iso = F.when(text.rlike(_ISO_TS), F.try_to_timestamp(text, F.lit("yyyy-MM-dd HH:mm:ss")))
    european = F.when(text.rlike(_EU_TS), F.try_to_timestamp(text, F.lit("dd/MM/yyyy HH:mm:ss")))
    return F.coalesce(iso, european)


def parse_decimal(column: Column, precision: int = 12, scale: int = 2) -> Column:
    """Parse a number with a decimal point or decimal comma. Anything else gives NULL."""
    text = F.regexp_replace(F.trim(column), ",", ".")
    return F.when(text.rlike(_DECIMAL), text.cast(f"decimal({precision},{scale})"))


def parse_double(column: Column) -> Column:
    """Parse a floating point number. Anything else gives NULL."""
    text = F.regexp_replace(F.trim(column), ",", ".")
    return F.when(text.rlike(_DECIMAL), text.cast("double"))


def parse_int(column: Column) -> Column:
    """Parse a whole number. Anything else gives NULL."""
    text = F.trim(column)
    return F.when(text.rlike(_INTEGER), text.cast("int"))


def standardise_status(column: Column, allowed: Sequence[str]) -> Column:
    """Lower-case a status, unify separators, and map unexpected values to ``unknown``."""
    text = F.regexp_replace(F.lower(F.trim(column)), "[- ]+", "_")
    return F.when(text.isin(*allowed), text).otherwise(F.lit("unknown"))


def standardise_species(column: Column) -> Column:
    """Map the many spellings of a species to one small list of values."""
    text = F.lower(F.trim(column))
    return (
        F.when(text.isin("dog", "canine"), F.lit("dog"))
        .when(text.isin("cat", "feline"), F.lit("cat"))
        .when(text.isin("rabbit", "bird"), text)
        .when(text.isNull() | (text == ""), F.lit(None).cast("string"))
        .otherwise(F.lit("other"))
    )


def standardise_sex(column: Column) -> Column:
    """Map ``M`` / ``male`` / ``F`` / ``female`` to ``male`` / ``female`` / ``unknown``."""
    text = F.lower(F.trim(column))
    return (
        F.when(text.isin("m", "male"), F.lit("male"))
        .when(text.isin("f", "female"), F.lit("female"))
        .otherwise(F.lit("unknown"))
    )


# --------------------------------------------------------------------------- #
# DataFrame helpers
# --------------------------------------------------------------------------- #
def with_raw_record(df: DataFrame) -> DataFrame:
    """Add ``_raw``: the business columns of the Bronze row as one JSON string."""
    business = [c for c in df.columns if not c.startswith("_")]
    return df.withColumn("_raw", F.to_json(F.struct(*[F.col(c) for c in business])))


def with_dq_failures(df: DataFrame, rules: dict[str, Column]) -> DataFrame:
    """Add ``_dq_failures``: the names of all rules that are not satisfied.

    Args:
        df: Input rows.
        rules: Rule name -> boolean column that must be TRUE for a valid row.
            A NULL result counts as a failure.
    """
    failed = [
        F.when(~F.coalesce(condition, F.lit(False)), F.lit(name))
        for name, condition in rules.items()
    ]
    return df.withColumn("_dq_failures", F.array_compact(F.array(*failed)))


def split_valid_rejected(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split checked rows into (valid rows, rejected rows).

    Valid rows lose the helper columns ``_dq_failures`` and ``_raw``.
    """
    failures = F.size("_dq_failures")
    valid = df.filter(failures == 0).drop("_dq_failures", "_raw")
    rejected = df.filter(failures > 0)
    return valid, rejected


def deduplicate_latest(
    df: DataFrame, keys: Sequence[str], order_columns: Sequence[str] = ("updated_at",)
) -> DataFrame:
    """Keep one row per business key: the most recent version.

    Rows are ranked by ``order_columns`` (newest first), then by ``_ingested_at``
    and ``_source_file`` so the result is stable.
    """
    ordering = [F.col(c).desc_nulls_last() for c in order_columns]
    ordering += [F.col("_ingested_at").desc(), F.col("_source_file").desc()]
    window = Window.partitionBy(*keys).orderBy(*ordering)
    return df.withColumn("_rn", F.row_number().over(window)).filter("_rn = 1").drop("_rn")


def to_quarantine(rejected: DataFrame, source_table: str, key_column: str) -> DataFrame:
    """Shape rejected rows for the shared quarantine table."""
    return rejected.select(
        F.lit(source_table).alias("source_table"),
        F.col(key_column).cast("string").alias("record_key"),
        F.col("_dq_failures").alias("failed_rules"),
        F.col("_raw").alias("raw_record"),
        "_source_file",
        "_batch_id",
        "_ingested_at",
        F.current_timestamp().alias("quarantined_at"),
    )


def _known_clinics(clinics: DataFrame) -> DataFrame:
    """Small lookup of valid clinics, safe to broadcast."""
    return F.broadcast(
        clinics.select(
            F.col("clinic_id").alias("_known_clinic_id"),
            F.col("currency").alias("_clinic_currency"),
        )
    )


def _date_in_business_range(date_column: Column, upper_bound: Column) -> Column:
    """TRUE when the date is NULL (checked by another rule) or inside the allowed range."""
    return date_column.isNull() | (
        (date_column >= F.lit(MIN_BUSINESS_DATE).cast("date")) & (date_column <= upper_bound)
    )


# --------------------------------------------------------------------------- #
# Entity transformations
# --------------------------------------------------------------------------- #
def transform_clinics(bronze: DataFrame) -> DataFrame:
    """Clean and type the clinic master data."""
    typed = with_raw_record(bronze).select(
        F.upper(clean_string(F.col("clinic_id"))).alias("clinic_id"),
        clean_string(F.col("clinic_name")).alias("clinic_name"),
        clean_string(F.col("city")).alias("city"),
        F.upper(clean_string(F.col("country_code"))).alias("country_code"),
        clean_string(F.col("country")).alias("country"),
        F.upper(clean_string(F.col("currency"))).alias("currency"),
        parse_date(F.col("opened_date")).alias("opened_date"),
        parse_int(F.col("num_vets")).alias("num_vets"),
        F.col("_ingested_at").alias("updated_at"),
        *META_COLUMNS,
        "_raw",
    )
    return with_dq_failures(
        typed,
        {
            "clinic_id_not_null": F.col("clinic_id").isNotNull(),
            "country_code_valid": F.col("country_code").isin(*VALID_COUNTRY_CODES),
            "currency_not_null": F.col("currency").isNotNull(),
        },
    )


def transform_owners(bronze: DataFrame) -> DataFrame:
    """Flatten the nested owner JSON and standardise contact details."""
    email = F.lower(clean_string(F.col("contact.email")))
    typed = with_raw_record(bronze).select(
        clean_string(F.col("owner_id")).alias("owner_id"),
        clean_string(F.col("first_name")).alias("first_name"),
        clean_string(F.col("last_name")).alias("last_name"),
        email.alias("email"),
        F.coalesce(email.rlike(_EMAIL), F.lit(False)).alias("email_valid"),
        clean_string(F.col("contact.phone")).alias("phone"),
        clean_string(F.col("address.street")).alias("street"),
        clean_string(F.col("address.city")).alias("city"),
        clean_string(F.col("address.postal_code")).alias("postal_code"),
        F.upper(clean_string(F.col("address.country_code"))).alias("country_code"),
        parse_timestamp(F.col("registered_at")).alias("registered_at"),
        (F.lower(F.trim(F.col("marketing_consent"))) == "true").alias("marketing_consent"),
        parse_timestamp(F.col("updated_at")).alias("updated_at"),
        *META_COLUMNS,
        "_raw",
    )
    return with_dq_failures(typed, {"owner_id_not_null": F.col("owner_id").isNotNull()})


def transform_pets(bronze: DataFrame) -> DataFrame:
    """Flatten the nested pet JSON, type the vaccinations array and standardise values."""
    vaccinations = F.transform(
        "vaccinations",
        lambda v: F.struct(
            clean_string(v["vaccine"]).alias("vaccine"), parse_date(v["date"]).alias("date")
        ),
    )
    typed = (
        with_raw_record(bronze)
        .select(
            clean_string(F.col("pet_id")).alias("pet_id"),
            F.initcap(clean_string(F.col("name"))).alias("name"),
            standardise_species(F.col("species")).alias("species"),
            clean_string(F.col("breed")).alias("breed"),
            standardise_sex(F.col("sex")).alias("sex"),
            parse_date(F.col("birth_date")).alias("birth_date"),
            clean_string(F.col("owner_id")).alias("owner_id"),
            F.upper(clean_string(F.col("home_clinic_id"))).alias("home_clinic_id"),
            clean_string(F.col("microchip.number")).alias("microchip_number"),
            parse_date(F.col("microchip.implanted_on")).alias("microchip_implanted_on"),
            parse_double(F.col("weight_kg")).alias("weight_kg"),
            vaccinations.alias("vaccinations"),
            parse_timestamp(F.col("updated_at")).alias("updated_at"),
            *META_COLUMNS,
            "_raw",
        )
        .withColumn("is_microchipped", F.col("microchip_number").isNotNull())
        .withColumn(
            "vaccination_count",
            F.when(F.col("vaccinations").isNull(), F.lit(0)).otherwise(F.size("vaccinations")),
        )
        .withColumn(
            "last_vaccination_date",
            F.array_max(F.transform("vaccinations", lambda v: v["date"])),
        )
    )
    weight = F.col("weight_kg")
    return with_dq_failures(
        typed,
        {
            "pet_id_not_null": F.col("pet_id").isNotNull(),
            "birth_date_not_in_future": F.col("birth_date").isNull()
            | (F.col("birth_date") <= F.to_date("_ingested_at")),
            "weight_in_range": weight.isNull() | ((weight > 0) & (weight <= MAX_PET_WEIGHT_KG)),
        },
    )


def transform_appointments(bronze: DataFrame, clinics: DataFrame) -> DataFrame:
    """Clean and type appointments and check the clinic reference.

    Args:
        bronze: Raw appointment rows.
        clinics: Silver clinics (the list of valid clinic ids).
    """
    typed = with_raw_record(bronze).select(
        clean_string(F.col("appointment_id")).alias("appointment_id"),
        F.upper(clean_string(F.col("clinic_id"))).alias("clinic_id"),
        clean_string(F.col("pet_id")).alias("pet_id"),
        parse_timestamp(F.col("appointment_ts")).alias("appointment_ts"),
        F.lower(clean_string(F.col("reason"))).alias("reason"),
        clean_string(F.col("vet_name")).alias("vet_name"),
        standardise_status(F.col("status"), APPOINTMENT_STATUSES).alias("status"),
        parse_int(F.col("duration_min")).alias("duration_min"),
        parse_timestamp(F.col("updated_at")).alias("updated_at"),
        *META_COLUMNS,
        "_raw",
    )
    joined = typed.withColumn("appointment_date", F.to_date("appointment_ts")).join(
        _known_clinics(clinics), F.col("clinic_id") == F.col("_known_clinic_id"), "left"
    )
    # Appointments can be booked ahead, so allow up to one year in the future.
    latest_allowed = F.add_months(F.to_date("_ingested_at"), 12)
    checked = with_dq_failures(
        joined,
        {
            "appointment_id_not_null": F.col("appointment_id").isNotNull(),
            "pet_id_not_null": F.col("pet_id").isNotNull(),
            "clinic_id_known": F.col("_known_clinic_id").isNotNull(),
            "appointment_ts_valid": F.col("appointment_ts").isNotNull(),
            "appointment_ts_in_range": _date_in_business_range(
                F.col("appointment_date"), latest_allowed
            ),
        },
    )
    return checked.drop("_known_clinic_id", "_clinic_currency")


def transform_invoices(bronze: DataFrame, clinics: DataFrame, fx_rates: DataFrame) -> DataFrame:
    """Clean and type invoices, check references and convert amounts to EUR.

    Args:
        bronze: Raw invoice rows.
        clinics: Silver clinics (valid clinic ids and their currency).
        fx_rates: Silver FX rates with one row per ``rate_date`` and ``currency``.
            ``rate_per_eur`` is the number of currency units for 1 EUR.
    """
    typed = with_raw_record(bronze).select(
        clean_string(F.col("invoice_id")).alias("invoice_id"),
        clean_string(F.col("appointment_id")).alias("appointment_id"),
        F.upper(clean_string(F.col("clinic_id"))).alias("clinic_id"),
        clean_string(F.col("pet_id")).alias("pet_id"),
        parse_date(F.col("invoice_date")).alias("invoice_date"),
        parse_decimal(F.col("amount")).alias("amount_local"),
        F.upper(clean_string(F.col("currency"))).alias("_currency_raw"),
        F.lower(clean_string(F.col("payment_method"))).alias("payment_method"),
        standardise_status(F.col("status"), INVOICE_STATUSES).alias("status"),
        parse_timestamp(F.col("updated_at")).alias("updated_at"),
        *META_COLUMNS,
        "_raw",
    )
    rates = F.broadcast(
        fx_rates.select(
            F.col("rate_date").alias("_fx_date"),
            F.col("currency").alias("_fx_currency"),
            F.col("rate_per_eur").alias("fx_rate"),
        )
    )
    converted = (
        typed.join(_known_clinics(clinics), F.col("clinic_id") == F.col("_known_clinic_id"), "left")
        # A missing currency falls back to the currency of the clinic.
        .withColumn("currency", F.coalesce("_currency_raw", "_clinic_currency"))
        .join(
            rates,
            (F.col("currency") == F.col("_fx_currency"))
            & (F.col("invoice_date") == F.col("_fx_date")),
            "left",
        )
        .withColumn(
            "amount_eur",
            F.round(F.try_divide(F.col("amount_local").cast("double"), F.col("fx_rate")), 2).cast(
                "decimal(14,2)"
            ),
        )
    )
    checked = with_dq_failures(
        converted,
        {
            "invoice_id_not_null": F.col("invoice_id").isNotNull(),
            "amount_not_null": F.col("amount_local").isNotNull(),
            "amount_not_negative": F.col("amount_local").isNull() | (F.col("amount_local") >= 0),
            "invoice_date_valid": F.col("invoice_date").isNotNull(),
            "invoice_date_in_range": _date_in_business_range(
                F.col("invoice_date"), F.to_date("_ingested_at")
            ),
            "clinic_id_known": F.col("_known_clinic_id").isNotNull(),
            "fx_rate_available": F.col("fx_rate").isNotNull()
            | F.col("invoice_date").isNull()
            | F.col("currency").isNull(),
        },
    )
    return checked.select(
        "invoice_id",
        "appointment_id",
        "clinic_id",
        "pet_id",
        "invoice_date",
        "amount_local",
        "currency",
        "fx_rate",
        "amount_eur",
        "payment_method",
        "status",
        "updated_at",
        *META_COLUMNS,
        "_raw",
        "_dq_failures",
    )


# --------------------------------------------------------------------------- #
# FX rates
# --------------------------------------------------------------------------- #
def build_fx_rates(
    bronze_api: DataFrame,
    bronze_manual: DataFrame,
    max_forward_fill_days: int = MAX_FORWARD_FILL_DAYS,
) -> DataFrame:
    """Build one clean rate per calendar day and currency.

    * The API JSON (a nested map ``date -> currency -> rate``) is exploded to rows.
    * Manual rates (e.g. RSD) are added.
    * If the same day arrives more than once, the latest delivery wins.
    * Central banks publish no rates on weekends and holidays, so gaps are
      filled with the last known rate (forward fill). After the last published
      rate the fill continues for ``max_forward_fill_days`` days only.
    * EUR itself gets the rate 1.0, so every invoice can use the same join.

    Returns:
        Columns ``rate_date, currency, rate_per_eur, is_forward_filled, source, _ingested_at``.
    """
    api = (
        bronze_api.select(F.explode("rates").alias("_date", "_by_currency"), "_ingested_at")
        .select(
            parse_date(F.col("_date")).alias("rate_date"),
            F.explode("_by_currency").alias("currency", "rate_per_eur"),
            "_ingested_at",
        )
        .withColumn("source", F.lit("frankfurter_ecb"))
    )
    manual = bronze_manual.select(
        parse_date(F.col("valid_from")).alias("rate_date"),
        F.upper(clean_string(F.col("currency"))).alias("currency"),
        parse_double(F.col("rate_per_eur")).alias("rate_per_eur"),
        "_ingested_at",
        F.coalesce(clean_string(F.col("source")), F.lit("manual")).alias("source"),
    )
    valid = api.unionByName(manual).filter(
        F.col("rate_date").isNotNull() & F.col("currency").isNotNull() & (F.col("rate_per_eur") > 0)
    )
    latest = Window.partitionBy("rate_date", "currency").orderBy(F.col("_ingested_at").desc())
    observed = valid.withColumn("_rn", F.row_number().over(latest)).filter("_rn = 1").drop("_rn")

    last_published = observed.agg(F.max("rate_date").alias("_last")).select(
        F.date_add("_last", max_forward_fill_days).alias("_calendar_end")
    )
    calendar = (
        observed.groupBy("currency")
        .agg(F.min("rate_date").alias("_first"))
        .crossJoin(last_published)
        .select("currency", F.explode(F.sequence("_first", "_calendar_end")).alias("rate_date"))
    )
    fill = (
        Window.partitionBy("currency")
        .orderBy("rate_date")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    filled = calendar.join(observed, ["currency", "rate_date"], "left").select(
        "rate_date",
        "currency",
        F.last("rate_per_eur", ignorenulls=True).over(fill).alias("rate_per_eur"),
        F.col("rate_per_eur").isNull().alias("is_forward_filled"),
        F.last("source", ignorenulls=True).over(fill).alias("source"),
        F.last("_ingested_at", ignorenulls=True).over(fill).alias("_ingested_at"),
    )
    euro = (
        filled.groupBy("rate_date")
        .agg(F.max("_ingested_at").alias("_ingested_at"))
        .select(
            "rate_date",
            F.lit("EUR").alias("currency"),
            F.lit(1.0).alias("rate_per_eur"),
            F.lit(False).alias("is_forward_filled"),
            F.lit("identity").alias("source"),
            "_ingested_at",
        )
    )
    return filled.unionByName(euro)
