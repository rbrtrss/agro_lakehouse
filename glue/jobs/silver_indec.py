"""
Glue ETL job: INDEC Bronze CSV → Silver Iceberg table (indec_exports).

Unpivots the wide INDEC matrix (1 row per date, 336 province×country columns)
into a long (year, province, country, fob_usd) fact table partitioned by year.

Job parameters (passed via --BRONZE_BUCKET, --SILVER_BUCKET, --SILVER_DB):
  BRONZE_BUCKET  e.g. agro-lakehouse-bronze
  SILVER_BUCKET  e.g. agro-lakehouse-silver
  SILVER_DB      e.g. agro_silver
"""

import sys

from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["BRONZE_BUCKET", "SILVER_BUCKET", "SILVER_DB"])
bronze_bucket = args["BRONZE_BUCKET"]
silver_bucket = args["SILVER_BUCKET"]
silver_db = args["SILVER_DB"]

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# ── 1. Read raw CSV from Bronze ───────────────────────────────────────────────

df_raw = (
    spark.read.option("header", "true")
    .option("inferSchema", "false")
    .csv(f"s3://{bronze_bucket}/source=indec/**/*.csv")
)

# ── 2. Identify provinces from *_total_* sentinel columns ────────────────────

all_cols = df_raw.columns
total_cols = [c for c in all_cols if "_total_" in c]
# e.g. "buenos_aires_total_buenos_aires" → province = "buenos_aires"
provinces = list({c.split("_total_")[0] for c in total_cols})

# Value columns: exclude date and all subtotal columns
value_cols = [c for c in all_cols if c != "indice_tiempo" and "_total_" not in c]

# ── 3. Resolve (province, country) for each value column ─────────────────────
# Match the longest province prefix to handle multi-word province names.

provinces_sorted = sorted(provinces, key=len, reverse=True)


def resolve_province_country(col_name: str) -> tuple[str, str]:
    for prov in provinces_sorted:
        prefix = prov + "_"
        if col_name.startswith(prefix):
            return prov, col_name[len(prefix) :]
    # Fallback: treat whole name as country with unknown province
    return "unknown", col_name


# ── 4. Unpivot wide → long using stack() ─────────────────────────────────────

stack_expr_parts = []
for col in value_cols:
    prov, country = resolve_province_country(col)
    # Escape single quotes in names
    prov_esc = prov.replace("'", "\\'")
    country_esc = country.replace("'", "\\'")
    stack_expr_parts.append(f"'{prov_esc}', '{country_esc}', `{col}`")

n = len(value_cols)
stack_expr = (
    f"stack({n}, {', '.join(stack_expr_parts)}) as (province, country, raw_value)"
)

df_long = df_raw.selectExpr("indice_tiempo", stack_expr)

# ── 5. Cast and derive columns ────────────────────────────────────────────────

df_silver = (
    df_long.withColumn("fecha", F.to_date(F.col("indice_tiempo")))
    .withColumn("year", F.year(F.col("fecha")))
    .withColumn("fob_usd", F.col("raw_value").cast("DECIMAL(18,4)"))
    .filter(F.col("fob_usd").isNotNull())
    .select("year", "province", "country", "fob_usd")
)

# ── 6. Create Silver Iceberg table if not exists ──────────────────────────────

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

# ── 7. Write — overwrite affected partitions (idempotent) ─────────────────────

(
    df_silver.writeTo(f"glue_catalog.{silver_db}.indec_exports")
    .tableProperty("format-version", "2")
    .overwritePartitions()
)
