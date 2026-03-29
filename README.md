# Lakehouse Contract Lab

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

## Quality Gates (Bronze → Silver)

| Rule | Rejected Label |
|------|----------------|
| `customer_id` must not be null | `missing_customer` |
| `region` must not be null | `missing_region` |
| `amount` must be > 0 | `non_positive_amount` |
| Dedup by `order_id`, keep newest | `stale_duplicate` |

## Data Contract Specification

Each layer boundary in the medallion pipeline enforces an explicit data contract. Contracts are defined in `data/quality_rules.json` and applied programmatically in `scripts/build_lakehouse_artifacts.py`.

**Validated Fields (Bronze to Silver boundary):**

| Field | Type | Contract Rule | Rejection Label |
|-------|------|---------------|-----------------|
| `customer_id` | `STRING` | Must not be null | `missing_customer` |
| `region` | `STRING` | Must not be null | `missing_region` |
| `amount` | `DOUBLE` | Must be strictly greater than zero | `non_positive_amount` |
| `order_id` | `STRING` | Deduplicated by `order_id`; only the row with the latest `order_ts` survives | `stale_duplicate` |

**Enforcement mechanism:** Quality rules are evaluated as a chained `WHEN` expression in Spark. Each row receives a `rejection_reason` column. Rows with a non-null rejection reason are partitioned into a rejected DataFrame that is persisted alongside the silver layer for auditability. Only rows passing all four gates graduate from bronze to silver.

**Rejected row tracking:** Every rejected row retains its full bronze envelope (`order_id`, `customer_id`, `region`, `amount`, `order_ts`) plus the `rejection_reason` label. Rejected rows are surfaced through the `/api/runtime/quality-report` endpoint and included in the `rejectedPreview` section of the quality report artifact.

**Silver to Gold boundary:** The gold layer aggregates accepted silver rows by `region`, producing KPI columns (`gross_revenue_usd`, `accepted_orders`, `completed_orders`, `pipeline_orders`, `distinct_customers`). The contract guarantees that no rejected row can influence gold-layer KPIs.

## Pipeline Metrics

Example output from a pipeline run against the 12-row synthetic dataset:

```
Pipeline Stage          Rows     Notes
──────────────────────  ───────  ──────────────────────────────
Source (CSV ingest)     12       Raw orders from data/source_orders.csv
Bronze                  12       All source rows preserved with ingested_at timestamp
Silver (accepted)       8        Passed all four quality gates
Silver (rejected)       4        Routed to review queue
Gold (region KPIs)      5        Aggregated by region

Quality Gate            Passed   Failed
──────────────────────  ───────  ──────
customer_present        11       1
region_present          11       1
positive_amount         11       1
latest_order_record     8        1

Overall pass rate:      66.67%
Rejection rate:         33.33%
Delta tables written:   3 (bronze_orders, silver_orders, gold_region_kpis)
```

Metrics are available programmatically at `/api/runtime/quality-report` and bundled into `artifacts/lakehouse-proof-pack.json`.

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

Note: on machines without Java 17, `make build` validates checked-in proof artifacts. The Docker image runs the full Spark + Delta rebuild.

## Core API

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Service health with proof-pack links |
| `GET` | `/api/runtime/quality-report` | Data quality gate results |
| `GET` | `/api/runtime/table-preview/{layer}` | Layer preview: `bronze` / `silver` / `gold` |

Additional artifacts (`lakehouse-proof-pack.json`, `source-pack.json`, `review-summary.json`) are generated in the `artifacts/` directory during pipeline builds and can be reviewed locally or exported to cloud storage.

## Deployment

**Snowflake** — set `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`. Gold KPIs are written via MERGE-based upserts to `app/snowflake_adapter.py`.

**Databricks Unity Catalog** — set `DATABRICKS_HOST` + auth (CLI profile, service-principal OAuth, or token). Gold KPIs land as Delta tables; catalog/schema auto-created. `DATABRICKS_CATALOG=workspace` for starter workspaces.

**AWS S3** — set `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `S3_ARTIFACT_BUCKET` to enable artifact upload.

**GCP Cloud Run** — Terraform config in `infra/terraform/`:
```bash
cd infra/terraform
cp terraform.tfvars.example terraform.tfvars
terraform init && terraform plan && terraform apply
```

All cloud integrations are env-var gated — the pipeline runs fully locally without any cloud dependency.

## Related Projects

For governed NL-to-SQL analytics on top of this data, see [Nexus-Hive](https://github.com/KIM3310/Nexus-Hive). For enterprise LLM governance, see [enterprise-llm-adoption-kit](https://github.com/KIM3310/enterprise-llm-adoption-kit).

## Tech Stack

Python · Spark · Delta Lake · FastAPI · Snowflake · Databricks (Unity Catalog, SDK Statement Execution API) · AWS S3 · GCP Cloud Run · Terraform · Docker · pytest

## License

MIT
