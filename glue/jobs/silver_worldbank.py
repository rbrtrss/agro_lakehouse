"""
Glue ETL job: World Bank Bronze CSV → Silver Iceberg table (worldbank_indicators).

Casts types, drops nulls, deduplicates on (country_code, indicator_code, year),
and writes partitioned by year.

Job parameters:
  BRONZE_BUCKET  e.g. agro-lakehouse-bronze
  SILVER_BUCKET  e.g. agro-lakehouse-silver
  SILVER_DB      e.g. agro_silver
"""

from __future__ import annotations

import pandas as pd

# ── Pure-pandas transform helper (importable without Glue/Spark) ──────────────

_OUTPUT_COLUMNS = [
    "country",
    "country_code",
    "indicator",
    "indicator_code",
    "year",
    "value",
]

_YEAR_MIN = 1960
_YEAR_MAX = 2030


def transform_worldbank_df(df: pd.DataFrame) -> pd.DataFrame:
    """Cast types, validate, and deduplicate World Bank indicator data.

    Args:
        df: Raw long-format DataFrame with columns:
            country, country_code, indicator, indicator_code, year, value

    Returns:
        Cleaned DataFrame with the same 6 canonical columns.
    """
    if df.empty:
        return pd.DataFrame(columns=_OUTPUT_COLUMNS)

    df = df.copy()

    # Cast value and drop nulls / non-positive values
    df["value"] = pd.to_numeric(df["value"], errors="coerce").astype(float)
    df = df.dropna(subset=["value"])
    df = df[df["value"] > 0]

    # Drop null or whitespace-only country_code / indicator_code
    df = df.dropna(subset=["country_code", "indicator_code"])
    df = df[df["country_code"].str.strip() != ""]
    df = df[df["indicator_code"].str.strip() != ""]

    # Cast year to int and filter range
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df = df.dropna(subset=["year"])
    df["year"] = df["year"].astype(int)
    df = df[(df["year"] >= _YEAR_MIN) & (df["year"] <= _YEAR_MAX)]

    # Deduplicate on natural key — keep first occurrence
    df = df.drop_duplicates(
        subset=["country_code", "indicator_code", "year"], keep="first"
    )

    return df[_OUTPUT_COLUMNS].reset_index(drop=True)


# ── Glue job entry point ──────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    from awsglue.context import GlueContext  # type: ignore[import]
    from awsglue.utils import getResolvedOptions  # type: ignore[import]
    from pyspark.context import SparkContext  # type: ignore[import]
    from pyspark.sql import functions as F  # type: ignore[import]
    from pyspark.sql.window import Window  # type: ignore[import]

    args = getResolvedOptions(sys.argv, ["BRONZE_BUCKET", "SILVER_BUCKET", "SILVER_DB"])
    bronze_bucket = args["BRONZE_BUCKET"]
    silver_bucket = args["SILVER_BUCKET"]
    silver_db = args["SILVER_DB"]

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

    # ── Configure Iceberg catalog BEFORE any catalog access ───────────────────
    spark.conf.set(
        "spark.sql.catalog.glue_catalog",
        "org.apache.iceberg.spark.SparkCatalog",
    )
    spark.conf.set(
        "spark.sql.catalog.glue_catalog.catalog-impl",
        "org.apache.iceberg.aws.glue.GlueCatalog",
    )
    spark.conf.set(
        "spark.sql.catalog.glue_catalog.io-impl",
        "org.apache.iceberg.aws.s3.S3FileIO",
    )
    spark.conf.set(
        "spark.sql.catalog.glue_catalog.warehouse",
        f"s3://{silver_bucket}/",
    )

    # ── 1. Read raw CSV from Bronze ───────────────────────────────────────────

    df_raw = (
        spark.read.option("header", "true")
        .option("inferSchema", "false")
        .option("recursiveFileLookup", "true")
        .csv(f"s3://{bronze_bucket}/source=worldbank/")
    )

    # ── 2. Cast types and validate ────────────────────────────────────────────

    df_typed = (
        df_raw.withColumn("year", F.col("year").cast("INT"))
        .withColumn("value", F.col("value").cast("DECIMAL(18,6)"))
        .filter(F.col("value").isNotNull())
        .filter(F.col("value") > 0)
        .filter(
            F.col("country_code").isNotNull() & (F.trim(F.col("country_code")) != "")
        )
        .filter(
            F.col("indicator_code").isNotNull()
            & (F.trim(F.col("indicator_code")) != "")
        )
        .filter(F.col("year").isNotNull())
        .filter((F.col("year") >= _YEAR_MIN) & (F.col("year") <= _YEAR_MAX))
    )

    # ── 3. Deduplicate on natural key — keep first occurrence ─────────────────

    dedup_window = Window.partitionBy("country_code", "indicator_code", "year").orderBy(
        F.monotonically_increasing_id()
    )

    df_silver = (
        df_typed.withColumn("_rn", F.row_number().over(dedup_window))
        .filter(F.col("_rn") == 1)
        .select(
            "country", "country_code", "indicator", "indicator_code", "year", "value"
        )
    )

    # ── 4. Create Silver Iceberg table if not exists ──────────────────────────

    spark.sql(f"""
      CREATE TABLE IF NOT EXISTS glue_catalog.{silver_db}.worldbank_indicators (
        country        STRING,
        country_code   STRING,
        indicator      STRING,
        indicator_code STRING,
        year           INT,
        value          DECIMAL(18,6)
      ) USING iceberg
      PARTITIONED BY (year)
      LOCATION 's3://{silver_bucket}/worldbank_indicators/'
    """)

    # ── 5. Write — overwrite affected partitions (idempotent) ─────────────────

    (
        df_silver.writeTo(f"glue_catalog.{silver_db}.worldbank_indicators")
        .tableProperty("format-version", "2")
        .overwritePartitions()
    )
