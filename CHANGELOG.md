# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-03-14

### Added

- Bronze, Silver, and Gold medallion pipeline using PySpark and Delta Lake.
- Four quality gates at the Bronze-to-Silver boundary: `customer_present`, `region_present`, `positive_amount`, `latest_order_record`.
- Rejected row tracking with labeled rejection reasons routed to a review queue.
- Delta table output for all three layers with version tracking via `_delta_log`.
- Gold-layer region KPI aggregation: `gross_revenue_usd`, `accepted_orders`, `completed_orders`, `pipeline_orders`, `distinct_customers`.
- Snowflake export adapter with MERGE-based upsert logic (`app/snowflake_adapter.py`).
- Databricks Unity Catalog export adapter with Statement Execution API (`app/databricks_adapter.py`).
- FastAPI service exposing `/health`, `/api/runtime/quality-report`, `/api/runtime/table-preview/{layer}`, and `/api/runtime/lakehouse-proof-pack`.
- SVG architecture board generation (`docs/lakehouse-contract-board.svg`).
- Synthetic 12-row order dataset with intentional quality violations for gate testing.
- JSON artifact generation: proof pack, quality report, review summary, source pack, layer previews.
- Docker and Docker Compose support for containerized execution.
- Terraform configuration for GCP Cloud Run deployment (`infra/terraform/`).
- CI pipeline with GitHub Actions: lint, test, build, smoke test, Docker build.
- Optional OpenAI-powered review summary enrichment (env-var gated).
- Prebuilt artifact validation for environments without a Java runtime.
