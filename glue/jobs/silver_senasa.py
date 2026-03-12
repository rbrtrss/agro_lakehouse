"""
Glue ETL job: SENASA Bronze CSV → Silver Iceberg table (senasa_certs).

Cleans and types phytosanitary certificate records, partitioned by year/month.

Job parameters:
  BRONZE_BUCKET  e.g. agro-lakehouse-bronze
  SILVER_BUCKET  e.g. agro-lakehouse-silver
  SILVER_DB      e.g. agro_silver
"""

from __future__ import annotations

import pandas as pd

# ── Pure-pandas transform helper (importable without Glue/Spark) ──────────────

_OUTPUT_COLUMNS = [
    "fecha",
    "oficina_cf",
    "provincia",
    "provincia_id",
    "pais_destino",
    "pais_destino_id",
    "pais_destino_id_iso_3166_1",
    "continente",
    "mercaderia_certificada",
    "transporte",
    "tn",
    "year",
    "month",
]

_STRING_COLS = [
    "oficina_cf",
    "provincia",
    "pais_destino",
    "pais_destino_id_iso_3166_1",
    "continente",
    "mercaderia_certificada",
    "transporte",
]


def transform_senasa_df(df: pd.DataFrame) -> pd.DataFrame:
    """Cast types, validate, deduplicate, and derive year/month for SENASA cert data.

    Args:
        df: Raw Bronze CSV DataFrame with all 11 original columns.

    Returns:
        Cleaned DataFrame with 13 canonical columns (11 original + year + month).
    """
    if df.empty:
        return pd.DataFrame(columns=_OUTPUT_COLUMNS)

    df = df.copy()

    # Trim all string columns
    for col in _STRING_COLS:
        df[col] = df[col].str.strip()

    # Parse fecha and drop unparseable rows
    df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
    df = df.dropna(subset=["fecha"])

    # Drop null or whitespace-only pais_destino
    df = df.dropna(subset=["pais_destino"])
    df = df[df["pais_destino"] != ""]

    # Cast tn to float (coerce errors → NaN); drop only negative values, keep NaN
    df["tn"] = pd.to_numeric(df["tn"], errors="coerce").astype(float)
    df = df[df["tn"].isna() | (df["tn"] >= 0)]

    # Cast integer ID columns
    df["provincia_id"] = pd.to_numeric(df["provincia_id"], errors="coerce")
    df["pais_destino_id"] = pd.to_numeric(df["pais_destino_id"], errors="coerce")

    # Derive partition columns
    df["year"] = df["fecha"].dt.year.astype(int)
    df["month"] = df["fecha"].dt.month.astype(int)

    # Drop exact duplicates
    df = df.drop_duplicates()

    return df[_OUTPUT_COLUMNS].reset_index(drop=True)


# ── Glue job entry point ──────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    from awsglue.context import GlueContext  # type: ignore[import]
    from awsglue.utils import getResolvedOptions  # type: ignore[import]
    from pyspark.context import SparkContext  # type: ignore[import]
    from pyspark.sql import functions as F  # type: ignore[import]

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
        .csv(f"s3://{bronze_bucket}/source=senasa/")
    )

    # ── 2. Cast and clean ─────────────────────────────────────────────────────

    df_silver = (
        df_raw
        # Trim all string columns
        .withColumn("oficina_cf", F.trim(F.col("oficina_cf")))
        .withColumn("provincia", F.trim(F.col("provincia")))
        .withColumn("pais_destino", F.trim(F.col("pais_destino")))
        .withColumn(
            "pais_destino_id_iso_3166_1", F.trim(F.col("pais_destino_id_iso_3166_1"))
        )
        .withColumn("continente", F.trim(F.col("continente")))
        .withColumn("mercaderia_certificada", F.trim(F.col("mercaderia_certificada")))
        .withColumn("transporte", F.trim(F.col("transporte")))
        # Type casts
        .withColumn("fecha", F.to_date(F.col("fecha")))
        .withColumn("provincia_id", F.col("provincia_id").cast("INT"))
        .withColumn("pais_destino_id", F.col("pais_destino_id").cast("INT"))
        .withColumn("tn", F.col("tn").cast("DECIMAL(18,4)"))
        # Validation: drop unparseable dates and null/empty pais_destino
        .filter(F.col("fecha").isNotNull())
        .filter(F.col("pais_destino").isNotNull() & (F.col("pais_destino") != ""))
        # Drop negative tn only (nulls are allowed)
        .filter(F.col("tn").isNull() | (F.col("tn") >= 0))
        # Partition columns
        .withColumn("year", F.year(F.col("fecha")))
        .withColumn("month", F.month(F.col("fecha")))
        # Drop exact duplicates
        .dropDuplicates()
        .select(*_OUTPUT_COLUMNS)
    )

    # ── 3. Create Silver Iceberg table if not exists ──────────────────────────

    spark.sql(f"""
      CREATE TABLE IF NOT EXISTS glue_catalog.{silver_db}.senasa_certs (
        fecha                      DATE,
        oficina_cf                 STRING,
        provincia                  STRING,
        provincia_id               INT,
        pais_destino               STRING,
        pais_destino_id            INT,
        pais_destino_id_iso_3166_1 STRING,
        continente                 STRING,
        mercaderia_certificada     STRING,
        transporte                 STRING,
        tn                         DECIMAL(18,4),
        year                       INT,
        month                      INT
      ) USING iceberg
      PARTITIONED BY (year, month)
      LOCATION 's3://{silver_bucket}/senasa_certs/'
    """)

    # ── 4. Write — overwrite affected partitions (idempotent) ─────────────────

    (
        df_silver.writeTo(f"glue_catalog.{silver_db}.senasa_certs")
        .tableProperty("format-version", "2")
        .overwritePartitions()
    )
