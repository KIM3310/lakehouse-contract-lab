# Medallion Architecture

This document describes the Bronze, Silver, and Gold layer implementation in Lakehouse Contract Lab.

## Overview

The pipeline follows the medallion (multi-hop) architecture pattern popularized by Databricks. Raw data enters the bronze layer, is cleaned and validated into silver, and aggregated into gold-level business KPIs. Each layer is persisted as a Delta Lake table with full transaction log history.

```
source_orders.csv
       |
       v
   ┌────────┐
   │ BRONZE │  Raw ingestion with metadata
   └───┬────┘
       |
       v
   ┌────────┐     ┌──────────────┐
   │ SILVER │────>│ Rejected Rows│  Quality gate failures
   └───┬────┘     └──────────────┘
       |
       v
   ┌────────┐
   │  GOLD  │  Region-level KPI aggregations
   └───┬────┘
       |
       v
  Export Layer (Snowflake / Databricks / S3)
```

## Bronze Layer

**Purpose:** Preserve the raw source data exactly as received, augmented with ingestion metadata.

**Input:** `data/source_orders.csv` -- a synthetic dataset of 12 order rows containing intentional quality violations (null customers, null regions, non-positive amounts, duplicate order IDs).

**Transformations:**
- `ingested_at` timestamp is attached to every row, recording when the pipeline processed the data.
- `source_rank` is assigned via `monotonically_increasing_id()` to provide a stable tiebreaker for deduplication.
- `order_ts` is cast from ISO-8601 string to a Spark `TimestampType`.

**Output columns:** `order_id`, `customer_id`, `region`, `channel`, `status`, `amount`, `currency`, `order_ts`, `ingested_at`, `source_rank`.

**Delta table:** Written to `artifacts/runtime_delta/bronze_orders/` with Delta Lake format, providing ACID guarantees and a commit log.

**Contract:** All source rows are preserved without filtering. The bronze layer never drops or modifies business data.

## Silver Layer

**Purpose:** Apply quality gates and deduplication to produce a clean, trusted dataset.

**Quality gates (applied at the Bronze-to-Silver boundary):**

| Priority | Rule | Rejection Label | Logic |
|----------|------|-----------------|-------|
| 1 | Customer must be present | `missing_customer` | `customer_id IS NULL` |
| 2 | Region must be present | `missing_region` | `region IS NULL` |
| 3 | Amount must be positive | `non_positive_amount` | `amount <= 0` |
| 4 | Deduplicate by order_id | `stale_duplicate` | Keep only `row_number() = 1` partitioned by `order_id`, ordered by `order_ts DESC, source_rank DESC` |

Rules are evaluated in priority order. The first matching rule determines the `rejection_reason` for a row. Deduplication is evaluated after field-level quality checks -- a row that fails a field check is rejected for the field violation, not as a stale duplicate.

**Accepted rows:** Rows with `rejection_reason IS NULL` form the silver layer.

**Rejected rows:** Rows with a non-null `rejection_reason` are retained in a separate DataFrame. They are included in the quality report artifact (`artifacts/quality-report.json`) under `rejectedPreview` and are accessible via the `/api/runtime/quality-report` endpoint.

**Output columns:** Same as bronze, plus `quality_issue`, `row_rank`, and `rejection_reason` (null for accepted rows).

**Delta table:** Written to `artifacts/runtime_delta/silver_orders/`.

## Gold Layer

**Purpose:** Aggregate accepted silver rows into region-level business KPIs for consumption by BI tools, Snowflake, and Databricks Unity Catalog.

**Transformation:** `GROUP BY region` with the following aggregations:

| Output Column | Aggregation |
|---------------|-------------|
| `gross_revenue_usd` | `ROUND(SUM(amount), 2)` |
| `accepted_orders` | `COUNT(*)` |
| `completed_orders` | `SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END)` |
| `pipeline_orders` | `SUM(CASE WHEN status = 'processing' THEN 1 ELSE 0 END)` |
| `distinct_customers` | `COUNT(DISTINCT customer_id)` |

**Contract:** Only silver-accepted rows contribute to gold KPIs. No rejected row can influence these aggregations. This is enforced structurally -- the gold DataFrame is computed from the filtered silver DataFrame, not from the full staged DataFrame.

**Delta table:** Written to `artifacts/runtime_delta/gold_region_kpis/`.

**Export:** Gold rows are exported to Snowflake (via MERGE upsert) and Databricks Unity Catalog (via Statement Execution API MERGE) when the respective environment variables are configured.

## Quality Gates Between Layers

The only quality gate boundary in this pipeline is between bronze and silver. The silver-to-gold transition is a pure aggregation with no additional filtering. This is intentional: once data passes the quality gates, it is trusted for all downstream computation.

**Gate results** are captured as expectation objects with `passed` and `failed` counts, stored in both the proof pack and quality report artifacts.

**Metrics tracked:**
- Per-gate pass/fail counts
- Overall quality pass rate percentage
- List of distinct rejection reasons
- Preview of rejected rows with their rejection labels

## Delta Lake Integration

All three layers are written as Delta Lake tables, providing:
- **ACID transactions** -- each layer write is atomic.
- **Schema enforcement** -- column types are locked on first write.
- **Version tracking** -- the pipeline reads `_delta_log/*.json` to report the latest commit version for each table in the proof pack artifact.
- **Time travel capability** -- previous versions of each table are accessible through the Delta log.
