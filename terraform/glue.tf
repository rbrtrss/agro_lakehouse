# Glue Catalog databases — one per medallion layer.
# Crawlers are deferred to Phase 2 when table schemas are known.

resource "aws_glue_catalog_database" "bronze" {
  name        = "agro_bronze"
  description = "Raw ingested data — Bronze layer"
}

resource "aws_glue_catalog_database" "silver" {
  name        = "agro_silver"
  description = "Cleaned and typed data — Silver layer (Iceberg)"
}

resource "aws_glue_catalog_database" "gold" {
  name        = "agro_gold"
  description = "Analytical marts — Gold layer (Iceberg via dbt)"
}
