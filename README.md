# Agro-Lakehouse: Argentina Agricultural Export Pipeline

> *"From Pampa to Port — A Cloud Data Lakehouse on Argentina's Agro-Exports"*

End-to-end cloud data lakehouse on AWS that ingests, transforms, and analyzes Argentina's agricultural export data. Showcases medallion architecture, Apache Iceberg, dbt, Airflow orchestration, Terraform IaC, and CI/CD.

---

## Architecture

```
INDEC / SENASA / Bolsa de Cereales / World Bank API
                    |
        Python Ingestion (Lambda / AWS Glue)
                    |
        S3 Bronze  (raw, partitioned by source/year/month)
                    |
        S3 Silver  (cleaned, typed, Iceberg tables)
                    |
        S3 Gold    (dbt models — star schema)
                    |
          AWS Athena Query Layer
                    |
         Apache Superset Dashboard
```

### Medallion Layers

| Layer | Description |
|---|---|
| **Bronze** | Raw files landed as-is, partitioned by `source/year/month` |
| **Silver** | Cleaned, typed, deduplicated — stored as Apache Iceberg tables |
| **Gold** | Business-ready star schema, built with dbt |

---

## Data Sources

| Source | Data | Format |
|---|---|---|
| [INDEC](https://datos.gob.ar/dataset?tags=exportaciones) | Export volumes & FOB values by product | CSV |
| [SENASA](https://datos.senasa.gob.ar) | Phytosanitary certificates, export destinations | CSV |
| Bolsa de Cereales | Grain harvest estimates by province | CSV/XLS |
| [World Bank](https://data.worldbank.org) | Global commodity prices (soy, wheat, corn) | API |

---

## Tech Stack

| Layer | Tool |
|---|---|
| Storage | AWS S3 |
| Table Format | Apache Iceberg |
| Ingestion | Python + AWS Glue / Lambda |
| Transformation | dbt Core (Athena adapter) |
| Query Engine | AWS Athena |
| Orchestration | Apache Airflow |
| Data Quality | dbt tests + Great Expectations |
| Infrastructure | Terraform |
| CI/CD | GitHub Actions |
| Visualization | Apache Superset |

---

## dbt Gold Layer Models

### Fact Table
- **`fct_exports`** — export volume (tons), FOB value (USD), destination country, product, date

### Dimension Tables
- **`dim_product`** — crop type, category (grains, oilseeds, livestock, derivatives)
- **`dim_destination`** — country, region, trade bloc (Mercosur, EU, China, ASEAN)
- **`dim_province`** — Argentine province, Pampa region flag, main crop
- **`dim_date`** — standard date spine (year, quarter, month, week)

### Key Analytical Questions
- Which crops generate the most USD per quarter?
- How does soy export volume correlate with global commodity prices?
- Which provinces lead wheat vs. corn exports?
- How has China's share of Argentine exports evolved YoY?
- How does ARS devaluation impact USD-denominated FOB values?

---

## Repository Structure

```
agro-lakehouse/
├── terraform/
│   ├── main.tf
│   ├── s3.tf
│   ├── glue.tf
│   ├── athena.tf
│   ├── iam.tf
│   └── variables.tf
├── ingestion/
│   ├── indec/
│   │   └── ingest_indec.py
│   ├── senasa/
│   │   └── ingest_senasa.py
│   ├── worldbank/
│   │   └── ingest_worldbank.py
│   └── utils/
│       └── s3_utils.py
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_indec_exports.sql
│   │   │   ├── stg_senasa_certs.sql
│   │   │   └── stg_worldbank_prices.sql
│   │   ├── intermediate/
│   │   │   └── int_exports_enriched.sql
│   │   └── marts/
│   │       ├── fct_exports.sql
│   │       ├── dim_product.sql
│   │       ├── dim_destination.sql
│   │       ├── dim_province.sql
│   │       └── dim_date.sql
│   └── tests/
│       └── generic/
├── airflow/
│   └── dags/
│       ├── ingestion_dag.py
│       └── dbt_run_dag.py
├── .github/
│   ├── workflows/
│   │   ├── ci.yml
│   │   ├── dbt_test.yml
│   │   └── terraform_plan.yml
│   └── pull_request_template.md
├── docs/
│   └── architecture_diagram.png
└── README.md
```

---

## Branching Strategy

```
main          ← stable, always deployable; protected
  └── feat/<scope>-<description>   ← feature / phase work
  └── fix/<description>            ← bug fixes
  └── infra/<description>          ← Terraform-only changes
  └── data/<description>           ← dbt model changes
  └── ci/<description>             ← CI/CD workflow changes
```

Branch protection on `main`: PR required, status checks must pass, no direct pushes.

---

## Getting Started

### Prerequisites
- Python >= 3.13
- AWS CLI configured with appropriate credentials
- Terraform >= 1.0
- dbt Core with Athena adapter

### Setup

```bash
# Clone and set up
git clone https://github.com/your-username/agro-lakehouse
cd agro-lakehouse

# Set up Python environment
uv sync

# Deploy infrastructure
cd terraform
terraform init
terraform plan
terraform apply

# Run ingestion
uv run ingestion/indec/ingest_indec.py

# Run dbt
cd dbt
dbt deps
dbt run
dbt test
```

---

## Build Roadmap

### Phase 1 — Foundation
- [ ] Set up AWS account, configure IAM roles and policies
- [ ] Write Terraform for S3 buckets (bronze/silver/gold), Glue Catalog, Athena workgroup
- [ ] Initialize GitHub repo, branch strategy, and CI skeleton
- [ ] Manually download and explore INDEC + SENASA sample files

### Phase 2 — Ingestion
- [ ] Write Python ingestion scripts for INDEC CSV files → S3 Bronze
- [ ] Write World Bank API client → S3 Bronze
- [ ] Set up AWS Glue job for large file processing
- [ ] Register Iceberg tables in Glue Catalog, verify Athena queries

### Phase 3 — Transformation
- [ ] Set up dbt project with Athena adapter
- [ ] Build staging models (`stg_*`) for each source
- [ ] Build intermediate model joining exports + prices
- [ ] Build Gold mart models (`fct_exports` + all dims)
- [ ] Add dbt tests: not_null, unique, accepted_values, relationships

### Phase 4 — Orchestration & Quality
- [ ] Write Airflow DAGs for ingestion scheduling
- [ ] Write Airflow DAG for dbt run + test
- [ ] Add Great Expectations suite on Silver layer
- [ ] Wire GitHub Actions: run dbt test on every PR, Terraform plan on infra changes

### Phase 5 — Polish & Portfolio
- [ ] Connect Superset to Athena, build dashboard (choropleth map + time series)
- [ ] Write architecture diagram (draw.io or Excalidraw)
- [ ] Write detailed README with setup instructions, architecture, and screenshots
- [ ] Record Loom walkthrough (5–10 min)
- [ ] Deploy cost estimate section in README

---

## Portfolio Differentiators

- **Argentina macro context** — FOB values in USD during ARS devaluation cycles tells a unique story
- **Multi-source joins** — INDEC + World Bank price correlation is impressive analytical modeling
- **Apache Iceberg** — ACID transactions, time travel queries, schema evolution
- **Geospatial layer** — Province-level choropleth map in Superset
- **SCD Type 2** on `dim_destination` to track shifting trade relationships over time
- **Terraform-only infra** — zero click-ops, fully reproducible environment
- **dbt lineage graph** screenshot in README signals mature engineering practices
