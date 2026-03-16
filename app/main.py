from __future__ import annotations

import json
from pathlib import Path

from fastapi import FastAPI, HTTPException

ROOT = Path(__file__).resolve().parents[1]
ARTIFACTS_DIR = ROOT / "artifacts"

app = FastAPI(
    title="Lakehouse Contract Lab",
    version="1.0.0",
    description=(
        "Spark + Delta public proof surface for medallion contracts, data quality "
        "gates, and reviewer-ready architecture posture."
    ),
)


def _load_json(filename: str) -> dict:
    path = ARTIFACTS_DIR / filename
    if not path.exists():
        raise HTTPException(
            status_code=503,
            detail=(
                f"Artifact {filename} is missing. Run "
                "python scripts/build_lakehouse_artifacts.py first."
            ),
        )
    return json.loads(path.read_text(encoding="utf-8"))


@app.get("/health")
def health() -> dict:
    proof = _load_json("lakehouse-proof-pack.json")
    return {
        "ok": True,
        "service": proof["service"],
        "status": proof["status"],
        "schema": "lakehouse-contract-health-v1",
        "generatedAt": proof["generatedAt"],
        "links": {
            "proofPack": "/api/runtime/lakehouse-proof-pack",
            "qualityReport": "/api/runtime/quality-report",
            "goldPreview": "/api/runtime/table-preview/gold",
        },
    }


@app.get("/api/runtime/lakehouse-proof-pack")
def lakehouse_proof_pack() -> dict:
    return _load_json("lakehouse-proof-pack.json")


@app.get("/api/runtime/quality-report")
def quality_report() -> dict:
    return _load_json("quality-report.json")


@app.get("/api/runtime/table-preview/{layer}")
def table_preview(layer: str) -> dict:
    mapping = {
        "bronze": "bronze-preview.json",
        "silver": "silver-preview.json",
        "gold": "gold-preview.json",
    }
    filename = mapping.get(layer.lower())
    if filename is None:
        raise HTTPException(status_code=404, detail="Unknown layer")
    return _load_json(filename)

