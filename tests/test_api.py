from pathlib import Path
import sys

from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.main import app

client = TestClient(app)


def test_health_exposes_proof_routes() -> None:
    response = client.get("/health")
    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["links"]["proofPack"] == "/api/runtime/lakehouse-proof-pack"


def test_lakehouse_proof_pack_has_delta_and_quality_signals() -> None:
    response = client.get("/api/runtime/lakehouse-proof-pack")
    assert response.status_code == 200
    payload = response.json()
    assert payload["schema"] == "lakehouse-proof-pack-v1"
    assert payload["summary"]["bronzeRows"] >= payload["summary"]["silverAcceptedRows"]
    assert payload["summary"]["goldRows"] >= 1
    assert any(table["layer"] == "gold" for table in payload["tables"])
    assert any(expectation["name"] == "positive_amount" for expectation in payload["governance"]["expectations"])


def test_quality_report_and_gold_preview_are_available() -> None:
    quality = client.get("/api/runtime/quality-report")
    assert quality.status_code == 200
    assert quality.json()["schema"] == "lakehouse-quality-report-v1"

    gold = client.get("/api/runtime/table-preview/gold")
    assert gold.status_code == 200
    payload = gold.json()
    assert payload["layer"] == "gold"
    assert len(payload["rows"]) >= 1
