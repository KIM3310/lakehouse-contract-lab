# Data Contracts

This document explains the contract-first approach used in Lakehouse Contract Lab.

## What Is a Data Contract?

A data contract is a formal agreement between a data producer and its consumers about the structure, quality, and semantics of the data at a layer boundary. In this project, contracts are enforced programmatically at the bronze-to-silver transition and structurally at the silver-to-gold transition.

## Contract Definitions

Contracts are defined in two places:

1. **`data/quality_rules.json`** -- declarative rule definitions specifying the rule name, target layer, human-readable description, and the rejection label applied when a row violates the rule.

2. **`scripts/build_lakehouse_artifacts.py`** -- executable implementation of each rule as a PySpark `WHEN` expression chain.

The declarative definitions serve as documentation and are bundled into the pipeline summary artifact. The executable implementation is the source of truth for enforcement.

### Bronze Layer Contract

The bronze layer contract is an ingestion guarantee:
- Every source row is preserved without modification to business fields.
- Metadata columns (`ingested_at`, `source_rank`) are appended.
- The `order_ts` field is cast to a proper timestamp type.
- No rows are dropped or filtered.

### Silver Layer Contract (Quality Gates)

Four rules define the bronze-to-silver boundary:

| Rule Name | Field | Condition | Rejection Label |
|-----------|-------|-----------|-----------------|
| `customer_present` | `customer_id` | Must not be null | `missing_customer` |
| `region_present` | `region` | Must not be null | `missing_region` |
| `positive_amount` | `amount` | Must be > 0 | `non_positive_amount` |
| `latest_order_record` | `order_id` | Only the most recent row per `order_id` survives | `stale_duplicate` |

Rules are evaluated in priority order. A row that violates multiple rules is tagged with the first matching rejection reason.

### Gold Layer Contract

The gold layer contract is a structural guarantee:
- Only silver-accepted rows feed into gold aggregations.
- The output schema is fixed: `region`, `gross_revenue_usd`, `accepted_orders`, `completed_orders`, `pipeline_orders`, `distinct_customers`.
- KPI calculations are deterministic given the same silver input.

## What Happens When Data Violates a Contract

When a row fails a quality gate at the bronze-to-silver boundary:

1. **Labeling:** The row receives a `rejection_reason` value corresponding to the violated rule (e.g., `missing_customer`).

2. **Partitioning:** The pipeline splits the staged DataFrame into two partitions:
   - `silver` -- rows where `rejection_reason IS NULL` (all gates passed).
   - `rejected` -- rows where `rejection_reason IS NOT NULL`.

3. **Persistence:** Both partitions are persisted. Silver rows are written to the silver Delta table. Rejected rows are included in the quality report artifact.

4. **Reporting:** The quality report (`artifacts/quality-report.json`) includes:
   - Per-gate pass/fail counts in the `expectations` array.
   - A `rejectedPreview` array showing the first 10 rejected rows with their order details and rejection reasons.
   - Summary statistics: `acceptedRows`, `failedRows`, `qualityPassRatePct`.

5. **API exposure:** Rejected row data is accessible at `/api/runtime/quality-report` for programmatic review.

## Rejected Row Review Queue

Rejected rows are not discarded. They form a review queue that allows data engineers and analysts to:

- Identify systematic data quality issues in upstream sources.
- Quantify the impact of quality violations on downstream KPIs.
- Trace specific rejected rows back to their source by `order_id`.
- Audit the pipeline's quality gate decisions.

The review queue is surfaced through:
- `artifacts/quality-report.json` -- the `rejectedPreview` field.
- `/api/runtime/quality-report` -- the API endpoint serving the same data.
- `artifacts/review-summary.json` -- a high-level summary of pipeline health.

## Contract Versioning

Contracts are versioned through the `schema` field in each artifact:
- `lakehouse-proof-pack-v1`
- `lakehouse-quality-report-v1`
- `lakehouse-review-summary-v1`
- `lakehouse-table-preview-v1`
- `lakehouse-source-pack-v1`

Schema version changes signal breaking contract changes to downstream consumers.

## Extending Contracts

To add a new quality rule:

1. Define the rule in `data/quality_rules.json` with a `name`, `layer`, `rule` description, and `rejected_label`.
2. Add the corresponding `WHEN` clause to the quality gate chain in `scripts/build_lakehouse_artifacts.py`.
3. Add a test row to `data/source_orders.csv` that triggers the new rule.
4. Add a validation case to `data/validation_cases.json`.
5. Run `make pipeline` to verify the rule fires correctly and metrics update.
