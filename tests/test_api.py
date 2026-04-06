"""API endpoint tests."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from fastapi.testclient import TestClient

ROOT: Path = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.main import app

client: TestClient = TestClient(app)


class TestHealthEndpoint:
    def test_health_returns_200(self) -> None:
        response = client.get("/health")
        assert response.status_code == 200

    def test_health_ok_flag(self) -> None:
        payload: dict[str, Any] = client.get("/health").json()
        assert payload["ok"] is True

    def test_health_exposes_proof_routes(self) -> None:
        payload: dict[str, Any] = client.get("/health").json()
        assert payload["openai_refresh"]["deploymentMode"] == "artifact-refresh-only"
        assert payload["reviewerFastPath"][0] == "/health"

    def test_health_schema_marker(self) -> None:
        payload: dict[str, Any] = client.get("/health").json()
        assert payload["schema"] == "lakehouse-contract-health-v1"


class TestQualityAndReviewEndpoints:
    def test_quality_report_returns_200(self) -> None:
        response = client.get("/api/runtime/quality-report")
        assert response.status_code == 200

    def test_quality_report_schema(self) -> None:
        payload: dict[str, Any] = client.get("/api/runtime/quality-report").json()
        assert payload["schema"] == "lakehouse-quality-report-v1"

    def test_review_summary_returns_404(self) -> None:
        response = client.get("/api/runtime/review-summary")
        assert response.status_code == 404

    def test_source_pack_returns_404(self) -> None:
        response = client.get("/api/runtime/source-pack")
        assert response.status_code == 404


class TestTablePreviewEndpoints:
    def test_gold_preview_returns_200(self) -> None:
        response = client.get("/api/runtime/table-preview/gold")
        assert response.status_code == 200

    def test_gold_preview_content(self) -> None:
        payload: dict[str, Any] = client.get("/api/runtime/table-preview/gold").json()
        assert payload["layer"] == "gold"
        assert len(payload["rows"]) >= 1

    def test_unknown_layer_returns_404(self) -> None:
        response = client.get("/api/runtime/table-preview/platinum")
        assert response.status_code == 404

    def test_bronze_preview_returns_200(self) -> None:
        response = client.get("/api/runtime/table-preview/bronze")
        assert response.status_code == 200

    def test_silver_preview_returns_200(self) -> None:
        response = client.get("/api/runtime/table-preview/silver")
        assert response.status_code == 200
