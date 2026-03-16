# Lakehouse Contract Lab

`Lakehouse Contract Lab` is a small but real Spark + Delta proof repo built to close the most obvious public gap in the data-platform track: explicit Delta execution with governed medallion surfaces that reviewers can inspect quickly.

## Portfolio posture

- Read this repo as public Spark/Delta execution proof, not as a dashboard demo.
- The main claim is simple: medallion contracts, data quality gates, and Delta-backed output can all be shown in one reviewer path.

## Best target-team fit

This repo is strongest for Snowflake solution engineering, Databricks field/platform teams, and broader data-platform architecture reviews.

| Team lens | What should stand out fast | Start here |
|---|---|---|
| Databricks / lakehouse platform | real Spark + Delta execution, Delta versions, medallion quality gates, reviewer-readable proof pack | `GET /api/runtime/lakehouse-proof-pack`, `GET /api/runtime/quality-report`, [`scripts/build_lakehouse_artifacts.py`](scripts/build_lakehouse_artifacts.py) |
| Snowflake / solutions architecture | governed KPI outputs, contract-first data flow, handoff-friendly review path | `GET /api/runtime/lakehouse-proof-pack`, `GET /api/runtime/table-preview/gold`, [`docs/lakehouse-contract-board.svg`](docs/lakehouse-contract-board.svg) |
| Big-tech data systems | explicit rejection handling, artifact generation, reproducible build path | `GET /health`, `GET /api/runtime/quality-report`, `artifacts/` |

## Reviewer Front Door

- **Data-platform engineer:** open `GET /api/runtime/lakehouse-proof-pack` -> `GET /api/runtime/quality-report` -> `GET /api/runtime/table-preview/gold` -> `scripts/build_lakehouse_artifacts.py`.
- **Solution architect:** open `GET /api/runtime/lakehouse-proof-pack` -> [`docs/lakehouse-contract-board.svg`](docs/lakehouse-contract-board.svg) -> `GET /api/runtime/table-preview/gold` -> `README.md`.
- **Recruiter / hiring manager:** read this README, then inspect [`docs/lakehouse-contract-board.svg`](docs/lakehouse-contract-board.svg) and `artifacts/lakehouse-proof-pack.json`.

## 60-second reviewer start

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
python scripts/build_lakehouse_artifacts.py
uvicorn app.main:app --host 127.0.0.1 --port 8096
```

Then read, in order:

1. `GET /api/runtime/lakehouse-proof-pack`
2. `GET /api/runtime/quality-report`
3. `GET /api/runtime/table-preview/gold`
4. [`docs/lakehouse-contract-board.svg`](docs/lakehouse-contract-board.svg)

## Why this repo exists

The public portfolio already had lakehouse framing. What it lacked was an explicit, runnable Spark/Delta proof that could survive a skeptical reviewer asking, "where is the actual Delta execution surface?"

This repo answers that directly:

- Bronze keeps the raw order envelope plus ingest metadata.
- Silver enforces customer, region, amount, and dedupe rules.
- Gold exposes region KPI outputs that are small enough to inspect but serious enough to discuss in architecture interviews.
- The API turns the generated artifacts into a fast reviewer path.

## Review surfaces

- Health: `GET /health`
- Proof pack: `GET /api/runtime/lakehouse-proof-pack`
- Quality report: `GET /api/runtime/quality-report`
- Review summary: `GET /api/runtime/review-summary`
- Bronze preview: `GET /api/runtime/table-preview/bronze`
- Silver preview: `GET /api/runtime/table-preview/silver`
- Gold preview: `GET /api/runtime/table-preview/gold`
- Visual board: [`docs/lakehouse-contract-board.svg`](docs/lakehouse-contract-board.svg)

## Repository layout

```text
lakehouse-contract-lab/
  app/
  artifacts/
  docs/
  scripts/
  tests/
```

## Notes

- This is a checked-in public proof surface, not a claim of production customer traffic.
- The strongest signal is explicit Spark + Delta execution plus reviewer-readable contract output.
- OpenAI, when configured, is used only for refresh-only reviewer summaries. This repo does not expose a public arbitrary inference route.
- Use this repo together with `Nexus-Hive` and `enterprise-llm-adoption-kit` for the complete Snowflake / Databricks / solutions architecture story.
