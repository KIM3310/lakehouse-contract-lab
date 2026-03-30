# Lakehouse Contract Lab
[![codecov](https://codecov.io/gh/KIM3310/lakehouse-contract-lab/branch/main/graph/badge.svg)](https://codecov.io/gh/KIM3310/lakehouse-contract-lab)

[![CI](https://github.com/KIM3310/lakehouse-contract-lab/actions/workflows/ci.yml/badge.svg)](https://github.com/KIM3310/lakehouse-contract-lab/actions/workflows/ci.yml)
[![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)

Production-grade **Spark + Delta Lake** medallion pipeline with explicit contract boundaries, data quality gates, and multi-cloud export. Demonstrates end-to-end data engineering from raw ingestion through governed KPI output to Snowflake and Databricks Unity Catalog.

## Architecture

```
Raw orders (CSV)
     ↓
  BRONZE  →  SILVER (quality gates + dedup)  →  GOLD (region KPI rollups)
                ↓                                      ↓
           Rejected rows                        Delta Tables
           (review queue)                              ↓
                                        Export Layer (Snowflake / Databricks / S3)
                                                       ↓
                                           FastAPI Service (/health /proof-pack /quality-report)
```

## Quality Gates (Bronze to Silver)

| Rule | Rejected Label |
|------|----------------|
| `customer_id` must not be null | `missing_customer` |
| `region` must not be null | `missing_region` |
| `amount` must be > 0 | `non_positive_amount` |
| Dedup by `order_id`, keep newest | `stale_duplicate` |

Quality rules are defined in `data/quality_rules.json` and applied as chained Spark expressions. Rows failing any gate are partitioned into a rejected DataFrame persisted alongside the silver layer. Only rows passing all gates graduate to silver. Rejected rows retain their full bronze envelope plus a `rejection_reason` label and are surfaced via `/api/runtime/quality-report`.

The gold layer aggregates accepted silver rows by region into KPI columns (`gross_revenue_usd`, `accepted_orders`, `completed_orders`, `pipeline_orders`, `distinct_customers`). No rejected row can influence gold-layer KPIs.

## Quick Start

```bash
git clone https://github.com/KIM3310/lakehouse-contract-lab.git && cd lakehouse-contract-lab
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python scripts/build_lakehouse_artifacts.py   # builds Delta tables + JSON artifacts
uvicorn app.main:app --host 127.0.0.1 --port 8096
open http://127.0.0.1:8096/docs
```

Docker:
```bash
cp .env.example .env && docker compose up --build
```

On machines without Java 17, `make build` validates checked-in proof artifacts. The Docker image runs the full Spark + Delta rebuild.

## Core API

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Service health with proof-pack links |
| `GET` | `/api/runtime/quality-report` | Data quality gate results |
| `GET` | `/api/runtime/table-preview/{layer}` | Layer preview: `bronze` / `silver` / `gold` |

## Deployment

**Snowflake** — set `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`. Gold KPIs are written via MERGE-based upserts.

**Databricks Unity Catalog** — set `DATABRICKS_HOST` + auth (CLI profile, service-principal OAuth, or token). Gold KPIs land as Delta tables; catalog/schema auto-created.

**AWS S3** — set `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `S3_ARTIFACT_BUCKET` to enable artifact upload.

**GCP Cloud Run** — Terraform config in `infra/terraform/`.

All cloud integrations are env-var gated — the pipeline runs fully locally without any cloud dependency.

## Related Projects

For governed NL-to-SQL analytics on top of this data, see [Nexus-Hive](https://github.com/KIM3310/Nexus-Hive). For enterprise LLM governance, see [enterprise-llm-adoption-kit](https://github.com/KIM3310/enterprise-llm-adoption-kit).

## Tech Stack

Python · Spark · Delta Lake · FastAPI · Snowflake · Databricks (Unity Catalog, SDK Statement Execution API) · AWS S3 · GCP Cloud Run · Terraform · Docker · pytest

## License

MIT
