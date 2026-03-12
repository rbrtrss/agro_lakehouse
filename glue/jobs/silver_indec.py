"""
Glue ETL job: INDEC Bronze CSV → Silver Iceberg table (indec_exports).

Unpivots the wide INDEC matrix (1 row per date, 336 province×country columns)
into a long (year, province, country, fob_usd) fact table partitioned by year.

Job parameters (passed via --BRONZE_BUCKET, --SILVER_BUCKET, --SILVER_DB):
  BRONZE_BUCKET  e.g. agro-lakehouse-bronze
  SILVER_BUCKET  e.g. agro-lakehouse-silver
  SILVER_DB      e.g. agro_silver
"""

from __future__ import annotations

import pandas as pd

# ── Pure-pandas transform helper (importable without Glue/Spark) ──────────────

_OUTPUT_COLUMNS = ["year", "province", "country", "fob_usd"]

_YEAR_MIN = 1990
_YEAR_MAX = 2030


def transform_indec_df(df: pd.DataFrame) -> pd.DataFrame:
    """Cast types, validate, and derive year column from long-format INDEC data.

    Args:
        df: Post-unpivot long DataFrame with columns:
            indice_tiempo, province, country, raw_value

    Returns:
        Cleaned DataFrame with columns: year, province, country, fob_usd
    """
    if df.empty:
        return pd.DataFrame(columns=_OUTPUT_COLUMNS)

    df = df.copy()

    # Cast fob_usd and drop nulls / non-positive values
    df["fob_usd"] = pd.to_numeric(df["raw_value"], errors="coerce").astype(float)
    df = df.dropna(subset=["fob_usd"])
    df = df[df["fob_usd"] > 0]

    # Drop null or whitespace-only province / country
    df = df.dropna(subset=["province", "country"])
    df = df[df["province"].str.strip() != ""]
    df = df[df["country"].str.strip() != ""]

    # Derive year and filter range
    df["year"] = pd.to_datetime(df["indice_tiempo"], errors="coerce").dt.year
    df = df.dropna(subset=["year"])
    df["year"] = df["year"].astype(int)
    df = df[(df["year"] >= _YEAR_MIN) & (df["year"] <= _YEAR_MAX)]

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
        .csv(f"s3://{bronze_bucket}/source=indec/")
    )

    # ── 2. Identify provinces from *_total_* sentinel columns ─────────────────

    all_cols = df_raw.columns
    total_cols = [c for c in all_cols if "_total_" in c]
    # e.g. "buenos_aires_total_buenos_aires" → province = "buenos_aires"
    provinces = list({c.split("_total_")[0] for c in total_cols})

    # Value columns: exclude date and all subtotal columns
    value_cols = [c for c in all_cols if c != "indice_tiempo" and "_total_" not in c]

    # ── 3. Resolve (province, country) for each value column ──────────────────
    # Match the longest province prefix to handle multi-word province names.

    provinces_sorted = sorted(provinces, key=len, reverse=True)

    def resolve_province_country(col_name: str) -> tuple[str, str]:
        for prov in provinces_sorted:
            prefix = prov + "_"
            if col_name.startswith(prefix):
                return prov, col_name[len(prefix) :]
        return "unknown", col_name

    # ── 4. Unpivot wide → long using stack() ──────────────────────────────────

    stack_expr_parts = []
    for col in value_cols:
        prov, country = resolve_province_country(col)
        prov_esc = prov.replace("'", "\\'")
        country_esc = country.replace("'", "\\'")
        stack_expr_parts.append(f"'{prov_esc}', '{country_esc}', `{col}`")

    n = len(value_cols)
    stack_expr = (
        f"stack({n}, {', '.join(stack_expr_parts)}) as (province, country, raw_value)"
    )

    df_long = df_raw.selectExpr("indice_tiempo", stack_expr)

    # ── 5. Cast, validate, and derive columns ─────────────────────────────────

    df_silver = (
        df_long.withColumn("fob_usd", F.col("raw_value").cast("DECIMAL(18,4)"))
        .withColumn("year", F.year(F.to_date(F.col("indice_tiempo"))))
        .filter(F.col("fob_usd").isNotNull())
        .filter(F.col("fob_usd") > 0)
        .filter(F.col("province").isNotNull() & (F.trim(F.col("province")) != ""))
        .filter(F.col("country").isNotNull() & (F.trim(F.col("country")) != ""))
        .filter((F.col("year") >= _YEAR_MIN) & (F.col("year") <= _YEAR_MAX))
        .select("year", "province", "country", "fob_usd")
    )

    # ── 6. Create Silver Iceberg table if not exists ──────────────────────────

    spark.sql(f"""
      CREATE TABLE IF NOT EXISTS glue_catalog.{silver_db}.indec_exports (
        year     INT,
        province STRING,
        country  STRING,
        fob_usd  DECIMAL(18,4)
      ) USING iceberg
      PARTITIONED BY (year)
      LOCATION 's3://{silver_bucket}/indec_exports/'
    """)

    # ── 7. Write — overwrite affected partitions (idempotent) ─────────────────

    (
        df_silver.writeTo(f"glue_catalog.{silver_db}.indec_exports")
        .tableProperty("format-version", "2")
        .overwritePartitions()
    )
