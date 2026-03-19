# Lakehouse Contract Lab

A Spark + Delta lakehouse pipeline demonstrating medallion architecture with explicit contract boundaries and data quality gates.

## Architecture

The pipeline follows the standard medallion pattern:

- **Bronze** -- raw order ingestion with metadata tracking
- **Silver** -- enforces customer, region, amount, and dedupe rules
- **Gold** -- aggregated region KPI outputs suitable for BI consumption

## Quick start

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
python scripts/build_lakehouse_artifacts.py
uvicorn app.main:app --host 127.0.0.1 --port 8096
```

## API endpoints

- `GET /health` -- service health check
- `GET /api/runtime/lakehouse-proof-pack` -- full pipeline output bundle
- `GET /api/runtime/quality-report` -- data quality gate results
- `GET /api/runtime/review-summary` -- pipeline execution summary
- `GET /api/runtime/table-preview/bronze` -- bronze layer sample
- `GET /api/runtime/table-preview/silver` -- silver layer sample
- `GET /api/runtime/table-preview/gold` -- gold layer sample

## Repository layout

```text
lakehouse-contract-lab/
  app/          # FastAPI service
  artifacts/    # Generated pipeline artifacts
  docs/         # Visual board (SVG)
  scripts/      # Build and artifact generation
  tests/        # Pytest suite
```

## Tests

```bash
pytest -v
```

## Notes

- OpenAI is used only for optional summary generation; no public inference route is exposed.
- The visual board at `docs/lakehouse-contract-board.svg` shows the contract flow across medallion layers.
