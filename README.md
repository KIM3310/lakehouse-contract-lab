# Lakehouse Contract Lab

[![CI](https://github.com/KIM3310/lakehouse-contract-lab/actions/workflows/ci.yml/badge.svg)](https://github.com/KIM3310/lakehouse-contract-lab/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/KIM3310/lakehouse-contract-lab/branch/main/graph/badge.svg)](https://codecov.io/gh/KIM3310/lakehouse-contract-lab)
[![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)

A production-grade **Spark + Delta Lake** medallion pipeline that enforces data contracts at every layer boundary, applies declarative quality gates, and exports governed KPIs to **Snowflake** and **Databricks Unity Catalog**. Built as a working reference implementation for contract-first data engineering.

---

## Architecture

```mermaid
flowchart LR
    subgraph Ingestion
        CSV["source_orders.csv<br/><i>12 rows, 4 intentional violations</i>"]
    end

    subgraph Bronze["Bronze Layer"]
        B_TABLE[("bronze_orders<br/>Delta Table")]
        B_CONTRACT["Contract:<br/>- Raw envelope preserved<br/>- ingested_at attached<br/>- source_rank for dedup"]
    end

    subgraph Silver["Silver Layer"]
        GATES{"Quality Gates<br/>4 rules"}
        S_TABLE[("silver_orders<br/>Delta Table")]
        REJECTED[/"Rejected Rows<br/>Review Queue"/]
        S_CONTRACT["Contract:<br/>- customer_id NOT NULL<br/>- region NOT NULL<br/>- amount > 0<br/>- Dedup by order_id"]
    end

    subgraph Gold["Gold Layer"]
        G_TABLE[("gold_region_kpis<br/>Delta Table")]
        G_CONTRACT["Contract:<br/>- Region-level KPIs only<br/>- Only accepted rows<br/>- Fixed output schema"]
    end

    subgraph Export["Export Layer"]
        SF["Snowflake<br/>MERGE Upsert"]
        DB["Databricks<br/>Unity Catalog"]
        S3["AWS S3<br/>Artifacts"]
    end

    subgraph API["FastAPI Service"]
        HEALTH["/health"]
        QR["/quality-report"]
        PREVIEW["/table-preview"]
    end

    CSV --> B_TABLE
    B_TABLE --> GATES
    GATES -->|"Passed"| S_TABLE
    GATES -->|"Failed"| REJECTED
    S_TABLE --> G_TABLE
    G_TABLE --> SF
    G_TABLE --> DB
    G_TABLE --> S3
    G_TABLE --> API
    REJECTED --> QR
```

---

## Hiring Fit and Proof Boundary

| Dimension | Details |
|-----------|---------|
| **Best fit roles** | Data Engineer, Analytics Engineer, Platform Engineer, Solutions Architect |
| **Strongest proof** | Medallion pipeline structure, quality gates, export adapters, reviewer-readable proof-pack APIs |
| **What is real** | Spark transforms, rejection logic, KPI rollups, Snowflake MERGE export logic, Databricks export bridges, local review surfaces |
| **What is bounded** | Live Snowflake and Databricks exports only activate when credentials are configured; the seeded business dataset is synthetic |

---

## Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| Compute | Apache Spark (PySpark 3.5) | Distributed transforms across medallion layers |
| Storage | Delta Lake 3.2 | ACID transactions, schema enforcement, time travel |
| API | FastAPI | Serve quality reports, table previews, health checks |
| Warehouse | Snowflake | Gold KPI export via MERGE-based upserts |
| Catalog | Databricks Unity Catalog | Delta table export via Statement Execution API |
| Object Storage | AWS S3 | Artifact upload target |
| IaC | Terraform | GCP Cloud Run deployment configuration |
| Container | Docker / Docker Compose | Reproducible full-stack execution |
| CI/CD | GitHub Actions | Lint, test, build, smoke test, Docker build |
| Quality | pytest (81+ tests), ruff | Test suite with coverage; linting and formatting |

---

## Quick Start

### Local (no Docker)

```bash
# Clone and set up
git clone https://github.com/KIM3310/lakehouse-contract-lab.git
cd lakehouse-contract-lab
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

# Run the full pipeline: lint, test, build artifacts
make pipeline

# Start the API server
uvicorn app.main:app --host 127.0.0.1 --port 8096
open http://127.0.0.1:8096/docs
```

### Docker (recommended for full Spark + Delta rebuild)

```bash
cp .env.example .env
docker compose up --build
# API available at http://localhost:8096/docs
```

### No Java Runtime?

If Java is not installed, the pipeline validates checked-in prebuilt artifacts instead of rebuilding Spark/Delta outputs. Tests and the API layer work without a JVM.

```bash
make test       # runs 81+ tests against prebuilt artifacts
make serve      # starts the API server
```

### Makefile Reference

| Command | Description |
|---------|-------------|
| `make install` | Create venv and install all dependencies |
| `make test` | Run the full pytest suite |
| `make lint` | Run ruff linter |
| `make build` | Run the medallion pipeline and generate artifacts |
| `make pipeline` | Full pipeline: lint + test + build |
| `make verify` | Full verification: pipeline + API smoke test |
| `make serve` | Start FastAPI dev server with hot reload |
| `make docker-run` | Run via Docker Compose |

---

## Quality Gates (Bronze to Silver)

| Rule | Field | Condition | Rejected Label |
|------|-------|-----------|----------------|
| `customer_present` | `customer_id` | Must not be null | `missing_customer` |
| `region_present` | `region` | Must not be null | `missing_region` |
| `positive_amount` | `amount` | Must be > 0 | `non_positive_amount` |
| `latest_order_record` | `order_id` | Dedup, keep newest | `stale_duplicate` |

Rules are defined declaratively in `data/quality_rules.json` and enforced as chained PySpark `WHEN` expressions. Failed rows land in a rejected DataFrame with a `rejection_reason` label, accessible at `/api/runtime/quality-report`. Rejected rows are never discarded -- they form a review queue for data engineers to audit upstream quality issues.

Gold aggregates accepted silver rows by region into KPI columns: `gross_revenue_usd`, `accepted_orders`, `completed_orders`, `pipeline_orders`, `distinct_customers`.

---

## Consolidated Operating Pattern

[Data platform operating patterns](docs/data-platform-operating-patterns.md) folds demo-pack and rollout-playbook material into the canonical contract-first path used by this repository.

---

## Core API

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Service health with proof-pack links |
| `GET` | `/api/runtime/quality-report` | Data quality gate results with rejected row preview |
| `GET` | `/api/runtime/table-preview/{layer}` | Layer preview: `bronze` / `silver` / `gold` |
| `GET` | `/api/runtime/pipeline-summary` | Pipeline metrics across all three layers |
| `GET` | `/api/runtime/export-status` | Snowflake, Databricks, S3 export configuration status |

---

## Deployment

All cloud integrations are env-var gated -- the project runs fully locally without any cloud credentials.

**Snowflake** -- set `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`. Gold KPIs are written via MERGE-based upserts to `LAKEHOUSE_LAB.GOLD.REGION_KPIS`.

**Databricks Unity Catalog** -- set `DATABRICKS_HOST` + auth (CLI profile, service-principal OAuth, or token). Gold KPIs land as Delta tables; catalog/schema auto-created.

**AWS S3** -- set `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `S3_ARTIFACT_BUCKET` to enable artifact upload.

**GCP Cloud Run** -- Terraform config in `infra/terraform/`.

---

## Project Structure

```
lakehouse-contract-lab/
|-- app/
|   |-- main.py                  # FastAPI app serving pipeline artifacts
|   |-- snowflake_adapter.py     # Snowflake MERGE-based export adapter
|   |-- databricks_adapter.py    # Databricks Unity Catalog export adapter
|   |-- resource_pack.py         # Source data and config loaders
|-- scripts/
|   |-- build_lakehouse_artifacts.py  # Full medallion pipeline (Bronze/Silver/Gold)
|-- data/
|   |-- source_orders.csv        # 12-row synthetic dataset with quality violations
|   |-- quality_rules.json       # Declarative quality gate definitions
|   |-- export_targets.json      # Export target configurations
|   |-- validation_cases.json    # Test validation cases
|-- artifacts/                   # Generated pipeline outputs (JSON + Delta)
|-- tests/                       # 81+ pytest tests (adapters, API, pipeline, resource pack)
|-- docs/
|   |-- adr/                     # Architecture Decision Records
|   |-- data-platform-operating-patterns.md # Consolidated rollout and demo patterns
|   |-- data-contracts.md        # Contract-first approach documentation
|   |-- medallion-architecture.md # Layer-by-layer architecture guide
|-- infra/terraform/             # GCP Cloud Run deployment
|-- .github/workflows/ci.yml     # CI: lint, test, build, smoke, Docker
```

---

## Latest Verified Snapshot

- **Verified on:** 2026-04-07
- **Command:** `make verify`
- **Outcome:** Passed locally; self-healing Python 3.11 bootstrap, 81 tests, lint, prebuilt artifact validation, and smoke checks completed successfully
- **Notes:** Snowflake export tests and Snowflake export bridge tests were rerun successfully in the repo venv, while fresh Spark assembly remains gated by local Java availability

---

## Related Projects

| Project | Description |
|---------|-------------|
| [Nexus-Hive](https://github.com/KIM3310/Nexus-Hive) | Governed NL-to-SQL analytics on top of this data |
| [enterprise-llm-adoption-kit](https://github.com/KIM3310/enterprise-llm-adoption-kit) | Enterprise LLM governance framework |

---

## License

MIT

## Cloud + AI Architecture

This repository includes a neutral cloud and AI engineering blueprint that maps the current proof surface to runtime boundaries, data contracts, model-risk controls, deployment posture, and validation hooks.

- [Cloud + AI architecture blueprint](docs/cloud-ai-architecture.md)
- [Machine-readable architecture manifest](architecture/blueprint.json)
- Validation command: `python3 scripts/validate_architecture_blueprint.py`
