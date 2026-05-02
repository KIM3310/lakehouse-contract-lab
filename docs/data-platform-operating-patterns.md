# Data Platform Operating Patterns

This document consolidates data-platform demo and onboarding playbooks into one implementation-oriented reference. It avoids product-specific positioning and focuses on contracts, quality gates, rollout sequencing, and proof artifacts.

## Canonical Flow

```text
source sample
  -> bronze envelope
  -> silver quality gates
  -> rejected-row queue
  -> gold KPI contract
  -> optional export adapter
  -> health and proof APIs
```

The repository keeps this flow runnable locally through deterministic fixtures. Live exports remain optional and must be enabled through environment configuration.

## Demo Pack Structure

| Layer | Evidence To Keep |
|---|---|
| Source | Synthetic rows with known defects and a short data dictionary. |
| Bronze | Raw preservation, ingestion metadata, and deduplication inputs. |
| Silver | Declarative validation rules with rejected-row reasons. |
| Gold | Stable KPI schema with accepted-row-only aggregates. |
| Export | Adapter status, dry-run safety, and clear credential boundaries. |
| API | Health, quality report, preview, pipeline summary, and export status. |

## Rollout Checklist

1. Define the smallest useful source sample.
2. Write explicit contracts before building transformations.
3. Add negative rows that prove each quality gate.
4. Keep rejected rows inspectable.
5. Generate proof artifacts during local builds.
6. Add optional adapters only after the local path is green.
7. Gate live integrations behind environment variables.

## Quality Gate Rules

Each rule should have:

- a stable ID
- the checked field
- the pass condition
- the rejected-row label
- at least one positive fixture
- at least one negative fixture

This keeps quality behavior easy to test and prevents dashboards from hiding data-contract failures behind aggregate metrics.

## Operating Boundary

The canonical project is the contract-first pipeline in this repository. Related demo and onboarding material should point here for the runnable core, then keep only domain-specific notes outside this repo when they cannot be generalized.
