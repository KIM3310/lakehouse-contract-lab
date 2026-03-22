# Lakehouse Contract Lab

[![CI](https://github.com/KIM3310/lakehouse-contract-lab/actions/workflows/ci.yml/badge.svg)](https://github.com/KIM3310/lakehouse-contract-lab/actions/workflows/ci.yml)
[![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)

A production-grade **Spark + Delta Lake** medallion pipeline with explicit contract boundaries, data quality gates, and multi-cloud deployment support. Built to demonstrate end-to-end data engineering from ingestion through governed KPI output.

---

## Architecture

```text
+------------------------------------------------------------------+
|                    Lakehouse Contract Lab                         |
+------------------------------------------------------------------+
|                                                                  |
|  +-----------+     +-------------+     +----------+              |
|  |  BRONZE   |---->|   SILVER    |---->|   GOLD   |              |
|  |           |     |             |     |          |              |
|  | Raw order |     | Quality     |     | Region   |              |
|  | ingestion |     | gates +     |     | KPI      |              |
|  | + metadata|     | dedup       |     | rollups  |              |
|  +-----------+     +------+------+     +-----+----+              |
|       |                   |                  |                   |
|       |            +------+------+           |                   |
|       |            |  Rejected   |           |                   |
|       |            |  rows for   |           |                   |
|       |            |  review     |           |                   |
|       |            +-------------+           |                   |
|       v                                      v                   |
|  +----------+                         +-----------+              |
|  |  Delta   |                         |  Delta    |              |
|  |  Tables  |                         |  Tables   |              |
|  +----------+                         +-----------+              |
|                                              |                   |
|       +--------------------------------------+--------+          |
|       |              Export Layer                      |          |
|       |   +------------+  +-------------+  +-------+  |          |
|       |   | Snowflake  |  | Databricks  |  |  S3   |  |          |
|       |   | (env-gated)|  | Unity Cat.  |  | (AWS) |  |          |
|       |   +------------+  +-------------+  +-------+  |          |
|       +-----------------------------------------------+          |
|                                                                  |
|  +------------------------------------------------------------+  |
|  |                    FastAPI Service                          |  |
|  |  /health  /proof-pack  /quality-report  /table-preview/*   |  |
|  |  /docs (Swagger UI)   /redoc (ReDoc)                       |  |
|  +------------------------------------------------------------+  |
+------------------------------------------------------------------+
```

### Quality Gates (Bronze to Silver)

| Rule | Description | Rejected Label |
|------|-------------|----------------|
| Customer present | `customer_id` must not be null | `missing_customer` |
| Region present | `region` must not be null | `missing_region` |
| Positive amount | `amount` must be > 0 | `non_positive_amount` |
| Latest record only | Deduplicates by `order_id`, keeps newest | `stale_duplicate` |

---

## Quick Start

### Local (Python)

```bash
# Clone and set up
git clone https://github.com/KIM3310/lakehouse-contract-lab.git
cd lakehouse-contract-lab
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Run the pipeline (builds Delta tables + JSON artifacts)
python scripts/build_lakehouse_artifacts.py

# Start the API server
uvicorn app.main:app --host 127.0.0.1 --port 8096

# Visit the interactive docs
open http://127.0.0.1:8096/docs
```

### Docker

```bash
# Copy and configure environment
cp .env.example .env

# Build and run
docker compose up --build

# Or use Make
make docker-build
make docker-run
```

The API will be available at `http://localhost:8096` with a healthcheck on `/health`.

> Local note: on machines without a working Java 17 runtime, `make build` / `make smoke` fall back to validating the checked-in proof artifacts. The Docker image still performs the full Spark + Delta rebuild.

---

## API Documentation

The FastAPI application serves interactive API docs automatically:

- **Swagger UI**: [http://localhost:8096/docs](http://localhost:8096/docs)
- **ReDoc**: [http://localhost:8096/redoc](http://localhost:8096/redoc)

### Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Service health with proof-pack links |
| `GET` | `/api/runtime/lakehouse-proof-pack` | Full pipeline output bundle |
| `GET` | `/api/runtime/quality-report` | Data quality gate results |
| `GET` | `/api/runtime/review-summary` | Pipeline execution summary |
| `GET` | `/api/runtime/table-preview/{layer}` | Layer preview (bronze/silver/gold) |

### curl Examples

```bash
# Health check
curl -s http://localhost:8096/health | python -m json.tool

# Full proof pack
curl -s http://localhost:8096/api/runtime/lakehouse-proof-pack | python -m json.tool

# Quality report
curl -s http://localhost:8096/api/runtime/quality-report | python -m json.tool

# Gold layer preview
curl -s http://localhost:8096/api/runtime/table-preview/gold | python -m json.tool

# Review summary
curl -s http://localhost:8096/api/runtime/review-summary | python -m json.tool
```

---

## Multi-Cloud Deployment

All cloud integrations are **env-var gated** -- they activate only when credentials are present. The pipeline runs fully locally without any cloud dependency.

### AWS S3 (Artifact Storage)

Set `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `S3_ARTIFACT_BUCKET` to enable artifact upload to S3. Infrastructure is defined in `infra/terraform/main.tf`.

### Snowflake (Gold KPI Export)

Set `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, and `SNOWFLAKE_PASSWORD` to enable writing gold KPIs to Snowflake. The adapter (`app/snowflake_adapter.py`) handles schema creation, table DDL, and MERGE-based upserts.

### Databricks Unity Catalog

Set `DATABRICKS_HOST` and `DATABRICKS_TOKEN` to enable writing gold KPIs as Delta tables in Unity Catalog. The adapter (`app/databricks_adapter.py`) manages catalog/schema creation and uses the Databricks SDK.

### GCP Cloud Run

Deploy the container to Cloud Run using the Terraform config in `infra/terraform/`. The service includes health probes, autoscaling, and public access.

```bash
cd infra/terraform
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars with your GCP project ID
terraform init && terraform plan && terraform apply
```

---

## Repository Layout

```text
lakehouse-contract-lab/
  app/
    main.py                  # FastAPI service with all endpoints
    snowflake_adapter.py     # Snowflake gold KPI connector
    databricks_adapter.py    # Databricks Unity Catalog connector
  artifacts/                 # Generated pipeline artifacts (JSON)
  docs/
    lakehouse-contract-board.svg  # Visual architecture board
  infra/
    terraform/               # AWS S3 + GCP Cloud Run IaC
  scripts/
    build_lakehouse_artifacts.py  # Full medallion pipeline
  tests/
    test_api.py              # API integration tests
    test_build_lakehouse.py  # Pipeline + utility unit tests
  .github/workflows/ci.yml  # CI: lint, test, Docker build, coverage
  Dockerfile                 # Python 3.11 + Java 17 container
  docker-compose.yml         # Single-command local deployment
  Makefile                   # Common dev workflow targets
  .env.example               # Environment variable template
```

---

## Development

### Make Targets

```bash
make install     # Create venv and install dependencies
make test        # Run pytest suite
make lint        # Run ruff linter
make build       # Build pipeline artifacts
make docker-build  # Build Docker image
make docker-run    # Run via Docker Compose
make pipeline    # Full pipeline: lint + test + build
make smoke       # Local API smoke on top of built artifacts
make verify      # Pipeline + runtime smoke
```

### Running Tests

```bash
# All tests
make test

# With coverage
pytest --cov=app --cov=scripts --cov-report=term-missing

# Specific test class
pytest tests/test_build_lakehouse.py::TestMedallionSourceContract -v
```

---

## Review / demo checklist

Suggested proof surfaces for reviewers:

- Swagger UI with all runtime endpoints
- sample `/health` response showing proof-pack links
- gold layer preview JSON payload
- `docs/lakehouse-contract-board.svg` architecture board
- Terraform plan / cloud-export notes from `infra/terraform/`

---

## Notes

- **OpenAI** is used only for optional summary generation; no public inference route is exposed.
- The visual board at `docs/lakehouse-contract-board.svg` shows the contract flow across medallion layers.
- All Delta tables are written locally by default; cloud export requires explicit opt-in via environment variables.
