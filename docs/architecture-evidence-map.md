# Architecture Guide - Lakehouse Contract Lab

Updated: 2026-05-30

Use this page as the short path through the repository. It keeps the architecture grounded in the code, docs, commands, and boundaries that are already present.

## Summary

| Field | Notes |
|---|---|
| Lane | B2B data quality and migration |
| Core idea | Contract-first medallion pipeline with quality gates and rejected-row evidence. |
| Primary reader | Data platform, BI, analytics engineering, and warehouse migration teams. |
| Stack | Python, Terraform, Docker |

## Open First

1. Start with the README fast path and architecture section.
2. Open `docs/service-launch-playbook.md` only when architectureing the product or service angle.
3. Check the commands below before making claims about quality.
4. Skim the CI workflows and fixture data before deeper implementation architecture.
5. Read the boundaries section before presenting the project externally.

## Checks

| Purpose | Command |
|---|---|
| Full local gate | `make verify` |
| Test suite | `make test` |

## CI

- .github/workflows/architecture-blueprint.yml
- .github/workflows/ci.yml
- .github/workflows/dependency-architecture.yml
- .github/workflows/repository-health.yml
- .github/workflows/repository-surface.yml
- .github/workflows/secret-scan.yml

## Evidence

- pytest/ruff-style local verification path
- infrastructure-as-code architecture surface
- containerized delivery path
- make verify passes
- CI passes without Spark hang
- Quality reports are inspectable

## Architecture Notes

| Possible offer | Working scope assumption |
|---|---|
| Data-contract starter pack | Scope after product intake |
| Quality-gate implementation | Scope after product intake |
| Warehouse migration readiness architecture | Scope after product intake |

## Boundaries

- Fixture data only
- Production lineage required
- Warehouse exports need scoped credentials

## Useful Metrics

- Rejected-row visibility
- Contract coverage
- Pipeline failure lead time
