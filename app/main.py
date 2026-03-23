"""FastAPI application exposing lakehouse proof-pack artifacts and contract surfaces.

Endpoints serve pre-built JSON artifacts from the medallion pipeline,
including the pipeline output, quality report, review summary, and layer previews.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException

from app.resource_pack import external_data_summary

logger = logging.getLogger(__name__)

ROOT: Path = Path(__file__).resolve().parents[1]
ARTIFACTS_DIR: Path = ROOT / "artifacts"

app = FastAPI(
    title="Lakehouse Contract Lab",
    version="1.0.0",
    description=(
        "Spark + Delta Lake medallion pipeline API with explicit contract boundaries, "
        "data quality gates, and multi-cloud export support.\n\n"
        "## Medallion Layers\n\n"
        "- **Bronze** -- raw order ingestion with metadata tracking\n"
        "- **Silver** -- quality gates (customer, region, amount) + deduplication\n"
        "- **Gold** -- region-level KPI aggregations for BI consumption\n\n"
        "## Quality Gates\n\n"
        "| Rule | Description |\n"
        "|------|-------------|\n"
        "| `customer_present` | customer_id must not be null |\n"
        "| `region_present` | region must not be null |\n"
        "| `positive_amount` | amount must be > 0 |\n"
        "| `latest_record` | dedup by order_id, keep newest |\n"
    ),
    openapi_tags=[
        {
            "name": "health",
            "description": "Service health and discovery endpoints.",
        },
        {
            "name": "pipeline",
            "description": "Pipeline artifact endpoints (proof pack, quality report, review summary).",
        },
        {
            "name": "resources",
            "description": "Synthetic source data, quality rules, and validation-case inspection surfaces.",
        },
        {
            "name": "preview",
            "description": "Table preview endpoints for each medallion layer.",
        },
    ],
    docs_url="/docs",
    redoc_url="/redoc",
    license_info={
        "name": "MIT",
    },
)

LAYER_ARTIFACT_MAP: dict[str, str] = {
    "bronze": "bronze-preview.json",
    "silver": "silver-preview.json",
    "gold": "gold-preview.json",
}


def _openai_refresh_contract() -> dict[str, Any]:
    """Return the OpenAI refresh contract configuration.

    Describes deployment mode, budget caps, and moderation settings
    for the optional OpenAI-powered artifact refresh feature.
    """
    api_key: str = (os.getenv("OPENAI_API_KEY") or "").strip()
    return {
        "deploymentMode": "artifact-refresh-only",
        "publicLiveApi": False,
        "liveModel": "",
        "refreshModel": (os.getenv("OPENAI_MODEL_REFRESH") or "gpt-4o").strip(),
        "dailyBudgetUsd": 0.0,
        "monthlyBudgetUsd": 120.0 if api_key else 0.0,
        "killSwitch": False,
        "moderationEnabled": True,
    }


def _load_json(filename: str) -> dict[str, Any]:
    """Load and parse a JSON artifact file from the artifacts directory.

    Args:
        filename: Name of the JSON file to load (e.g. ``"lakehouse-proof-pack.json"``).

    Returns:
        Parsed JSON content as a dictionary.

    Raises:
        HTTPException: If the artifact file does not exist (HTTP 503).
    """
    path: Path = ARTIFACTS_DIR / filename
    if not path.exists():
        logger.error("Artifact missing: %s (looked at %s)", filename, path)
        raise HTTPException(
            status_code=503,
            detail=(
                f"Artifact {filename} is missing. Run "
                "python scripts/build_lakehouse_artifacts.py first."
            ),
        )
    logger.debug("Loading artifact: %s", filename)
    return json.loads(path.read_text(encoding="utf-8"))


@app.get("/health", tags=["health"])
def health() -> dict[str, Any]:
    """Return service health status with links to all proof-pack endpoints."""
    logger.info("Health check requested")
    proof: dict[str, Any] = _load_json("lakehouse-proof-pack.json")
    return {
        "ok": True,
        "service": proof["service"],
        "status": proof["status"],
        "schema": "lakehouse-contract-health-v1",
        "generatedAt": proof["generatedAt"],
        "reviewerFastPath": [
            "/health",
            "/api/runtime/lakehouse-proof-pack",
            "/api/runtime/source-pack",
            "/api/runtime/quality-report",
            "/api/runtime/review-summary",
            "/api/runtime/table-preview/gold",
        ],
        "openai_refresh": _openai_refresh_contract(),
        "links": {
            "proofPack": "/api/runtime/lakehouse-proof-pack",
            "sourcePack": "/api/runtime/source-pack",
            "qualityReport": "/api/runtime/quality-report",
            "reviewSummary": "/api/runtime/review-summary",
            "goldPreview": "/api/runtime/table-preview/gold",
        },
    }


@app.get("/api/runtime/lakehouse-proof-pack", tags=["pipeline"])
def lakehouse_proof_pack() -> dict[str, Any]:
    """Return the full lakehouse proof-pack artifact including pipeline summary, Delta table metadata, governance expectations, and platform fit assessments."""
    logger.info("Serving lakehouse-proof-pack")
    return _load_json("lakehouse-proof-pack.json")


@app.get("/api/runtime/quality-report", tags=["pipeline"])
def quality_report() -> dict[str, Any]:
    """Return the quality report with expectation results and rejected row previews."""
    logger.info("Serving quality-report")
    return _load_json("quality-report.json")


@app.get("/api/runtime/review-summary", tags=["pipeline"])
def review_summary() -> dict[str, Any]:
    """Return the review summary for reviewer handoff."""
    logger.info("Serving review-summary")
    return _load_json("review-summary.json")


@app.get("/api/runtime/source-pack", tags=["resources"])
def source_pack() -> dict[str, Any]:
    """Return the synthetic source/resource pack used to build the medallion proof."""
    logger.info("Serving source-pack")
    payload = _load_json("source-pack.json")
    payload["externalData"] = external_data_summary()
    return payload


@app.get("/api/runtime/table-preview/{layer}", tags=["preview"])
def table_preview(layer: str) -> dict[str, Any]:
    """Return a preview of rows from the specified medallion layer.

    Args:
        layer: One of ``bronze``, ``silver``, or ``gold``.

    Raises:
        HTTPException: If the layer name is not recognized (HTTP 404).
    """
    filename: str | None = LAYER_ARTIFACT_MAP.get(layer.lower())
    if filename is None:
        logger.warning("Unknown layer requested: %s", layer)
        raise HTTPException(status_code=404, detail="Unknown layer")
    logger.info("Serving table-preview for layer=%s", layer)
    return _load_json(filename)
