# ADR-001: Delta Lake Over Plain Parquet

## Status

Accepted

## Date

2026-03-14

## Context

The Lakehouse Contract Lab pipeline writes medallion-layer tables (bronze, silver, gold) to persistent storage. We needed to choose a storage format for these tables. The two main candidates were:

1. **Plain Apache Parquet** -- the de facto columnar format for analytical workloads.
2. **Delta Lake** -- an open table format built on Parquet that adds a transaction log layer.

The pipeline is designed as a portfolio project targeting Data Platform roles at Snowflake and Databricks, where Delta Lake is a core technology. However, the technical merits justified the choice independently.

## Decision

We chose **Delta Lake** as the storage format for all three medallion layers.

## Rationale

### ACID Transactions

Plain Parquet writes are not atomic. A failed write can leave partial files on disk, resulting in corrupted or incomplete tables. Delta Lake wraps each write in a transaction backed by a JSON commit log (`_delta_log/`), ensuring that readers always see a consistent snapshot. This is critical for the pipeline's quality gate enforcement -- a partial silver write could allow un-validated rows to be visible to the gold aggregation.

### Time Travel

Delta Lake retains previous versions of each table through its commit log. This allows:
- Auditing pipeline runs by querying historical table states.
- Debugging quality gate regressions by comparing silver snapshots across runs.
- Reproducing gold KPI values at a specific point in time.

The pipeline tracks and reports the latest Delta version for each table in the proof pack artifact, making version history a first-class part of the pipeline's contract surface.

### MERGE Support

The export adapters for Snowflake and Databricks use MERGE (upsert) semantics to write gold KPIs. Delta Lake's native MERGE operation maps directly to this pattern. With plain Parquet, implementing idempotent upserts would require reading the existing table, deduplicating in application code, and overwriting -- a pattern that is both error-prone and non-atomic.

### Schema Enforcement

Delta Lake enforces the table schema on write. If the pipeline code is modified to produce a column with an incompatible type, the write fails immediately rather than silently producing a schema-drifted file. This provides an additional safety net beyond the application-level quality gates.

### Ecosystem Alignment

- **Databricks** uses Delta Lake as its native table format. The Databricks Unity Catalog export adapter creates Delta tables directly. Using Delta locally mirrors the production format.
- **Snowflake** supports reading Delta Lake tables via external tables and Iceberg integration. The pipeline's Delta output can serve as a staging area for Snowflake ingestion.
- **Spark** includes first-class Delta Lake support via the `delta-spark` package, requiring only a configuration change to the SparkSession builder.

## Consequences

### Positive

- Atomic writes prevent partial/corrupt table states.
- Version tracking is available out of the box via `_delta_log`.
- Schema enforcement catches column-type regressions at write time.
- The local development format matches the Databricks production format.
- MERGE semantics are natively supported for the export layer.

### Negative

- Delta Lake adds a dependency on the `delta-spark` package and requires the Delta Spark session extension to be configured.
- The `_delta_log` directory adds storage overhead (minimal for this dataset size).
- Environments without Java cannot execute the full pipeline and must rely on prebuilt artifacts.

### Mitigations

- The Java/Spark dependency is mitigated by the `validate_prebuilt_artifacts()` fallback, which allows tests and the API layer to run against checked-in JSON artifacts without a JVM.
- Docker support provides a reproducible environment with all dependencies pre-installed.

## Alternatives Considered

### Plain Parquet

Simpler dependency chain but lacks transactions, schema enforcement, and time travel. Would require custom application logic to achieve the same guarantees.

### Apache Iceberg

Another open table format with similar capabilities. Delta Lake was preferred due to tighter Databricks ecosystem integration and the availability of the `delta-spark` Python package for local development.

### Apache Hudi

Provides upsert capabilities but has a heavier operational footprint and less alignment with the target Snowflake/Databricks platforms.
