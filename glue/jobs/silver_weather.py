"""
Glue ETL job: Weather Bronze CSV → Silver Iceberg table (weather_daily_readings).

Casts types, drops nulls, deduplicates on (date, province), derives year/month
partition columns, and writes partitioned by (year, month).

Job parameters:
  BRONZE_BUCKET  e.g. agro-lakehouse-bronze
  SILVER_BUCKET  e.g. agro-lakehouse-silver
  SILVER_DB      e.g. agro_silver
"""

from __future__ import annotations

import pandas as pd

# ── Pure-pandas transform helper (importable without Glue/Spark) ──────────────

_NUMERIC_COLS = [
    "latitude",
    "longitude",
    "temp_max_c",
    "temp_min_c",
    "precipitation_mm",
    "wind_speed_max_kmh",
    "evapotranspiration_mm",
]

_OUTPUT_COLUMNS = [
    "date",
    "province",
    "latitude",
    "longitude",
    "temp_max_c",
    "temp_min_c",
    "precipitation_mm",
    "wind_speed_max_kmh",
    "evapotranspiration_mm",
    "year",
    "month",
]


def transform_weather_df(df: pd.DataFrame) -> pd.DataFrame:
    """Cast types, drop nulls, deduplicate, and derive year/month columns.

    Args:
        df: Raw Bronze-style DataFrame with string columns.

    Returns:
        Cleaned DataFrame with 11 canonical columns.
    """
    if df.empty:
        return pd.DataFrame(columns=_OUTPUT_COLUMNS)

    df = df.copy()

    # Cast date and drop nulls on key columns
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df = df.dropna(subset=["date", "province"])

    # Cast numeric columns to float
    for col in _NUMERIC_COLS:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)

    # Deduplicate on (date, province) — keep first occurrence
    df = df.drop_duplicates(subset=["date", "province"], keep="first")

    # Derive partition columns
    df["year"] = pd.to_datetime(df["date"]).dt.year.astype(int)
    df["month"] = pd.to_datetime(df["date"]).dt.month.astype(int)

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
        .csv(f"s3://{bronze_bucket}/source=weather/")
    )

    # ── 2. Cast types and drop nulls ──────────────────────────────────────────

    df_typed = (
        df_raw.withColumn("date", F.col("date").cast("DATE"))
        .withColumn("latitude", F.col("latitude").cast("DECIMAL(8,5)"))
        .withColumn("longitude", F.col("longitude").cast("DECIMAL(8,5)"))
        .withColumn("temp_max_c", F.col("temp_max_c").cast("DECIMAL(6,2)"))
        .withColumn("temp_min_c", F.col("temp_min_c").cast("DECIMAL(6,2)"))
        .withColumn("precipitation_mm", F.col("precipitation_mm").cast("DECIMAL(8,2)"))
        .withColumn(
            "wind_speed_max_kmh", F.col("wind_speed_max_kmh").cast("DECIMAL(6,2)")
        )
        .withColumn(
            "evapotranspiration_mm", F.col("evapotranspiration_mm").cast("DECIMAL(8,2)")
        )
        .filter(F.col("date").isNotNull() & F.col("province").isNotNull())
    )

    # ── 3. Deduplicate on (date, province) — keep first occurrence ────────────

    dedup_window = Window.partitionBy("date", "province").orderBy(
        F.monotonically_increasing_id()
    )

    df_silver = (
        df_typed.withColumn("_rn", F.row_number().over(dedup_window))
        .filter(F.col("_rn") == 1)
        .withColumn("year", F.year(F.col("date")).cast("INT"))
        .withColumn("month", F.month(F.col("date")).cast("INT"))
        .select(*_OUTPUT_COLUMNS)
    )

    # ── 4. Create Silver Iceberg table if not exists ──────────────────────────

    spark.sql(f"""
      CREATE TABLE IF NOT EXISTS glue_catalog.{silver_db}.weather_daily_readings (
        date                   DATE,
        province               STRING,
        latitude               DECIMAL(8,5),
        longitude              DECIMAL(8,5),
        temp_max_c             DECIMAL(6,2),
        temp_min_c             DECIMAL(6,2),
        precipitation_mm       DECIMAL(8,2),
        wind_speed_max_kmh     DECIMAL(6,2),
        evapotranspiration_mm  DECIMAL(8,2),
        year                   INT,
        month                  INT
      ) USING iceberg
      PARTITIONED BY (year, month)
      LOCATION 's3://{silver_bucket}/weather_daily_readings/'
    """)

    # ── 5. Write — overwrite affected partitions (idempotent) ─────────────────

    (
        df_silver.writeTo(f"glue_catalog.{silver_db}.weather_daily_readings")
        .tableProperty("format-version", "2")
        .overwritePartitions()
    )
