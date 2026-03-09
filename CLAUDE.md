# CLAUDE.md — Agro-Lakehouse Project Guide

## Project Overview

End-to-end AWS data lakehouse for Argentina's agricultural export data. Medallion architecture (Bronze/Silver/Gold) using Apache Iceberg, dbt, Airflow, and Terraform.

## Repository Layout

```
agro-lakehouse/
├── terraform/          # IaC — S3, Glue, Athena, IAM
├── ingestion/          # Python scripts per data source
│   ├── indec/
│   ├── senasa/
│   ├── worldbank/
│   └── utils/
├── dbt/                # Transformation layer
│   └── models/
│       ├── staging/    # stg_* models, one per source
│       ├── intermediate/ # int_* enrichment joins
│       └── marts/      # fct_exports + dim_* tables
├── airflow/dags/       # Ingestion + dbt orchestration DAGs
└── .github/workflows/  # CI: dbt test on PR, terraform plan on infra changes
```

## Architecture — Medallion Layers

| Layer | Storage | Format | Owner |
|---|---|---|---|
| Bronze | `s3://agro-lakehouse-bronze/` | Raw CSV/JSON | ingestion/ scripts |
| Silver | `s3://agro-lakehouse-silver/` | Apache Iceberg | Glue jobs |
| Gold | `s3://agro-lakehouse-gold/` | Apache Iceberg via dbt | dbt marts/ |

Partitioning convention on Bronze: `source=<name>/year=<YYYY>/month=<MM>/`

## Data Sources

| Name | Key | Notes |
|---|---|---|
| INDEC | `indec` | CSV exports; FOB values in USD |
| SENASA | `senasa` | Phytosanitary certs; destination countries |
| Bolsa de Cereales | `bolsa` | Province-level harvest estimates |
| World Bank | `worldbank` | Commodity price API (soy, wheat, corn) |

## dbt Conventions

- **Staging models** (`stg_<source>_<entity>.sql`): select + rename only, no business logic
- **Intermediate models** (`int_<description>.sql`): joins and enrichment
- **Mart models**: `fct_exports` (fact) + `dim_product`, `dim_destination`, `dim_province`, `dim_date`
- Run `dbt test` after every model change — not_null, unique, relationships tests must pass
- `dim_destination` uses SCD Type 2 to track trade-bloc shifts over time

## Python / Ingestion Conventions

- Python >= 3.13 (see `.python-version`)
- Each source has its own script under `ingestion/<source>/ingest_<source>.py`
- Shared S3 helpers live in `ingestion/utils/s3_utils.py`
- Scripts must be idempotent — re-running should not duplicate Bronze data

## Infrastructure (Terraform)

- All AWS resources defined in `terraform/` — no manual console changes
- Key resources: S3 buckets (bronze/silver/gold), Glue Catalog, Athena workgroup, IAM roles
- Always run `terraform plan` before `terraform apply`
- CI runs `terraform plan` automatically on PRs touching `terraform/`

## CI/CD (GitHub Actions)

| Workflow | Trigger | Action |
|---|---|---|
| `ci.yml` | Every PR + push to `main` | `ruff check` + `ruff format --check` |
| `dbt_test.yml` | PR touching `dbt/` | `dbt deps && dbt compile` (syntax check; full run requires AWS) |
| `terraform_plan.yml` | PR touching `terraform/` | `terraform init && terraform validate && terraform plan` |

## Git Commits

**ALWAYS** use the generating-commit-messages skill before any `git commit`:

1. Run `git diff --staged` to review staged changes
2. Check if any items in the README.md Build Roadmap can now be checked off — if so, update them and stage the change before committing
3. Invoke `@/home/roberto/.claude/skills/generating-commit-messages/SKILL.md`
4. Generate a message following its format (summary < 50 chars, detailed body)
5. Never use generic messages; never include Co-Authored-By or Claude Code lines

## Pull Requests

**ALWAYS** use the creating-pull-requests skill before any `gh pr create`:

1. Run `git log main..HEAD --oneline` and `git diff main...HEAD` to review all changes
2. Invoke `@/home/roberto/.claude/skills/creating-pull-requests/SKILL.md`
3. Generate a title + structured body (What / Why / Changes / Checklist)
4. Never use generic titles; never include Co-Authored-By or Claude Code lines

## Package Management

Use `uv` exclusively — never `pip` or bare `python` for package operations.

```bash
uv add <package>       # add dependency
uv sync                # install all deps from pyproject.toml
uv run <script>        # run a script in the project environment
```

## Common Commands

```bash
# Ingestion
uv run ingestion/indec/ingest_indec.py
uv run ingestion/worldbank/ingest_worldbank.py

# dbt
cd dbt
dbt deps
dbt run --select staging
dbt run --select marts
dbt test

# Terraform
cd terraform
terraform init
terraform plan
terraform apply

# Airflow (local dev)
airflow dags test ingestion_dag 2024-01-01
airflow dags test dbt_run_dag 2024-01-01
```

## Key Analytical Goals

- Which crops generate the most USD per quarter?
- How does soy export volume correlate with global commodity prices?
- Which provinces lead wheat vs. corn exports?
- How has China's share of Argentine exports evolved YoY?
- How does ARS devaluation impact USD-denominated FOB values?

## Development Phases

| Phase | Focus | Status |
|---|---|---|
| 1 | Foundation — AWS setup, Terraform, IAM | [ ] |
| 2 | Ingestion — Python scripts, Glue, Iceberg registration | [ ] |
| 3 | Transformation — dbt staging, intermediate, gold models | [ ] |
| 4 | Orchestration & Quality — Airflow DAGs, Great Expectations, CI | [ ] |
| 5 | Polish — Superset dashboard, architecture diagram, README | [ ] |
