"""
Glue ETL job: SENASA Bronze CSV → Silver Iceberg table (senasa_certs).

Cleans and types phytosanitary certificate records, partitioned by year/month.

Job parameters:
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
    .csv(f"s3://{bronze_bucket}/source=senasa/**/*.csv")
)

# ── 2. Cast and clean ─────────────────────────────────────────────────────────

string_cols = [
    "oficina_cf",
    "provincia",
    "pais_destino",
    "pais_destino_id_iso_3166_1",
    "continente",
    "mercaderia_certificada",
    "transporte",
]

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
    # Partition columns
    .withColumn("year", F.year(F.col("fecha")))
    .withColumn("month", F.month(F.col("fecha")))
    # Drop exact duplicates
    .dropDuplicates()
    .select(
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
    )
)

# ── 3. Create Silver Iceberg table if not exists ──────────────────────────────

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

# ── 4. Write — overwrite affected partitions (idempotent) ─────────────────────

(
    df_silver.writeTo(f"glue_catalog.{silver_db}.senasa_certs")
    .tableProperty("format-version", "2")
    .overwritePartitions()
)
