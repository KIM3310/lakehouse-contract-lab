# ADR-002: Data Contracts Enforced at Layer Boundaries

## Status

Accepted

## Date

2026-03-14

## Context

The Lakehouse Contract Lab pipeline uses a medallion architecture with three layers (bronze, silver, gold). Data flows from raw ingestion through quality validation to business-level KPI aggregation. We needed to decide where and how to enforce data quality constraints.

Three approaches were considered:

1. **Schema-only validation** -- rely on Delta Lake's schema enforcement to catch type mismatches at write time, with no business-rule validation.
2. **End-to-end validation** -- validate all rules at ingestion time and reject bad rows before they enter the pipeline.
3. **Layer-boundary contracts** -- define explicit contracts at each layer transition, with programmatic enforcement at the bronze-to-silver boundary and structural enforcement at the silver-to-gold boundary.

## Decision

We chose **layer-boundary contracts** with enforcement concentrated at the bronze-to-silver transition. Each layer has a documented contract specifying what guarantees it provides to downstream consumers.

## Rationale

### Separation of Concerns

Each layer has a single, well-defined responsibility:

- **Bronze contract**: Preserve the raw source envelope without modification. Append ingestion metadata. Never drop rows.
- **Silver contract**: Apply quality gates (customer present, region present, positive amount, dedup). Partition rows into accepted and rejected. Provide a clean, trusted dataset.
- **Gold contract**: Aggregate only accepted silver rows into a fixed KPI schema. No additional filtering or validation.

This separation means that each layer can be tested, debugged, and reasoned about independently. A quality gate failure is always a silver-layer concern; a KPI calculation issue is always a gold-layer concern.

### Rejected Row Preservation

By concentrating validation at the bronze-to-silver boundary rather than at ingestion, we preserve the ability to:

- **Audit** rejected rows against the original source data (both are in the pipeline).
- **Re-process** rejected rows if quality rules change (the raw data is always available in bronze).
- **Quantify** the impact of quality violations on downstream KPIs (rejected row counts feed the quality report).

If validation happened at ingestion time, rejected rows would be discarded before entering the pipeline, losing traceability.

### Declarative Rule Definitions

Quality rules are defined in `data/quality_rules.json` as structured objects with a name, target layer, human-readable description, and rejection label. This declarative approach provides:

- **Documentation as code**: Rule definitions serve as living documentation that stays synchronized with enforcement logic.
- **Auditability**: The proof-pack artifact includes the full rule set, allowing reviewers to inspect the quality contract without reading Spark code.
- **Extensibility**: Adding a new rule requires only a JSON entry and a corresponding `WHEN` clause, rather than restructuring the pipeline.

### Priority-Ordered Evaluation

Rules are evaluated in a defined priority order using PySpark's chained `WHEN` expressions. A row that violates multiple rules is tagged with the first matching rejection reason. This produces deterministic, explainable rejection labels rather than an ambiguous list of all violations.

### Structural Gold-Layer Enforcement

The gold layer does not apply additional quality gates. Instead, it enforces its contract structurally: the gold DataFrame is computed exclusively from the filtered silver DataFrame (where `rejection_reason IS NULL`). This makes it impossible for a rejected row to influence gold KPIs, regardless of any code changes to the aggregation logic.

## Consequences

### Positive

- Each layer's contract is independently testable and documentable.
- Rejected rows are preserved for auditing and re-processing.
- Quality rules are declarative and extensible without pipeline restructuring.
- Gold KPIs are structurally isolated from quality-failed data.
- The quality report artifact provides full transparency into gate decisions.

### Negative

- Quality gate logic is split between two locations: declarative definitions in JSON and executable `WHEN` expressions in the build script. These must be kept in sync manually.
- The priority-ordered single-label approach means a row with multiple violations only reports the first one. Fixing that violation may reveal a second one on the next pipeline run.
- Adding gate enforcement at the silver-to-gold boundary in the future would require refactoring the current aggregation flow.

### Mitigations

- The test suite includes contract tests that verify the source dataset contains rows triggering each quality rule, ensuring the declarative and executable definitions stay aligned.
- The single-label trade-off is intentional: it produces cleaner rejection reports and simpler root-cause analysis for data stewards.

## Alternatives Considered

### Schema-Only Validation

Relying solely on Delta Lake's schema enforcement would catch type mismatches but not business-rule violations (null customer IDs, non-positive amounts). This was rejected because the pipeline's value proposition depends on demonstrating business-level quality gates.

### End-to-End Validation at Ingestion

Validating and rejecting rows before they enter bronze would simplify the pipeline but eliminate the audit trail. Bronze's contract of preserving all source rows would be violated, and re-processing after rule changes would require re-ingesting from the source.

### Multi-Label Rejection

Tagging each row with all violated rules (instead of just the first) would provide more information but would complicate the rejection report and make it harder to prioritize fixes. The current approach was chosen for operational clarity.
