"""API endpoint integration tests for the Lakehouse Contract Lab FastAPI application.

These tests use FastAPI's TestClient to exercise all endpoints against
pre-built artifact files in the ``artifacts/`` directory.
"""

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


# ---------------------------------------------------------------------------
# Health endpoint
# ---------------------------------------------------------------------------


class TestHealthEndpoint:
    """Tests for the /health endpoint."""

    def test_health_returns_200(self) -> None:
        """Health check must return HTTP 200."""
        response = client.get("/health")
        assert response.status_code == 200

    def test_health_ok_flag(self) -> None:
        """Health payload must set ok=True."""
        payload: dict[str, Any] = client.get("/health").json()
        assert payload["ok"] is True

    def test_health_exposes_proof_routes(self) -> None:
        """Health payload must include links to all proof-pack routes."""
        payload: dict[str, Any] = client.get("/health").json()
        assert payload["openai_refresh"]["deploymentMode"] == "artifact-refresh-only"
        assert payload["reviewerFastPath"][0] == "/health"
        assert payload["links"]["proofPack"] == "/api/runtime/lakehouse-proof-pack"
        assert payload["links"]["reviewSummary"] == "/api/runtime/review-summary"

    def test_health_schema_marker(self) -> None:
        """Health payload must declare its schema version."""
        payload: dict[str, Any] = client.get("/health").json()
        assert payload["schema"] == "lakehouse-contract-health-v1"


# ---------------------------------------------------------------------------
# Proof pack endpoint
# ---------------------------------------------------------------------------


class TestProofPackEndpoint:
    """Tests for the /api/runtime/lakehouse-proof-pack endpoint."""

    def test_proof_pack_returns_200(self) -> None:
        """Proof pack endpoint must return HTTP 200."""
        response = client.get("/api/runtime/lakehouse-proof-pack")
        assert response.status_code == 200

    def test_proof_pack_schema(self) -> None:
        """Proof pack must declare the correct schema version."""
        payload: dict[str, Any] = client.get("/api/runtime/lakehouse-proof-pack").json()
        assert payload["schema"] == "lakehouse-proof-pack-v1"

    def test_proof_pack_has_delta_and_quality_signals(self) -> None:
        """Proof pack must include bronze >= silver rows, gold >= 1, and quality expectations."""
        payload: dict[str, Any] = client.get("/api/runtime/lakehouse-proof-pack").json()
        assert payload["summary"]["bronzeRows"] >= payload["summary"]["silverAcceptedRows"]
        assert payload["summary"]["goldRows"] >= 1
        assert any(table["layer"] == "gold" for table in payload["tables"])
        assert any(
            expectation["name"] == "positive_amount"
            for expectation in payload["governance"]["expectations"]
        )

    def test_proof_pack_contains_governance(self) -> None:
        """Proof pack must include the governance block with expectations."""
        payload: dict[str, Any] = client.get("/api/runtime/lakehouse-proof-pack").json()
        assert "governance" in payload
        assert "expectations" in payload["governance"]
        assert isinstance(payload["governance"]["expectations"], list)

    def test_proof_pack_contains_resource_pack_summary(self) -> None:
        """Proof pack must expose the synthetic resource pack summary."""
        payload: dict[str, Any] = client.get("/api/runtime/lakehouse-proof-pack").json()
        assert "resourcePack" in payload
        assert payload["resourcePack"]["source_row_count"] == 12


# ---------------------------------------------------------------------------
# Quality report and review summary
# ---------------------------------------------------------------------------


class TestQualityAndReviewEndpoints:
    """Tests for the quality-report and review-summary endpoints."""

    def test_quality_report_returns_200(self) -> None:
        """Quality report endpoint must return HTTP 200."""
        response = client.get("/api/runtime/quality-report")
        assert response.status_code == 200

    def test_quality_report_schema(self) -> None:
        """Quality report must declare the correct schema version."""
        payload: dict[str, Any] = client.get("/api/runtime/quality-report").json()
        assert payload["schema"] == "lakehouse-quality-report-v1"

    def test_review_summary_returns_200(self) -> None:
        """Review summary endpoint must return HTTP 200."""
        response = client.get("/api/runtime/review-summary")
        assert response.status_code == 200

    def test_review_summary_schema(self) -> None:
        """Review summary must declare the correct schema version."""
        payload: dict[str, Any] = client.get("/api/runtime/review-summary").json()
        assert payload["schema"] == "lakehouse-review-summary-v1"

    def test_source_pack_returns_200(self) -> None:
        """Source pack endpoint must return HTTP 200."""
        response = client.get("/api/runtime/source-pack")
        assert response.status_code == 200

    def test_source_pack_contains_validation_cases(self) -> None:
        """Source pack must expose validation cases and quality rules."""
        payload: dict[str, Any] = client.get("/api/runtime/source-pack").json()
        assert payload["schema"] == "lakehouse-source-pack-v1"
        assert len(payload["validationCases"]) >= 4
        assert len(payload["qualityRules"]) >= 4


# ---------------------------------------------------------------------------
# Table preview endpoints
# ---------------------------------------------------------------------------


class TestTablePreviewEndpoints:
    """Tests for the /api/runtime/table-preview/{layer} endpoint."""

    def test_gold_preview_returns_200(self) -> None:
        """Gold layer preview must return HTTP 200."""
        response = client.get("/api/runtime/table-preview/gold")
        assert response.status_code == 200

    def test_gold_preview_content(self) -> None:
        """Gold preview must include the correct layer tag and non-empty rows."""
        payload: dict[str, Any] = client.get("/api/runtime/table-preview/gold").json()
        assert payload["layer"] == "gold"
        assert len(payload["rows"]) >= 1

    def test_unknown_layer_returns_404(self) -> None:
        """Requesting an unknown medallion layer must return HTTP 404."""
        response = client.get("/api/runtime/table-preview/platinum")
        assert response.status_code == 404

    def test_bronze_preview_returns_200(self) -> None:
        """Bronze preview endpoint must return HTTP 200."""
        response = client.get("/api/runtime/table-preview/bronze")
        assert response.status_code == 200

    def test_silver_preview_returns_200(self) -> None:
        """Silver preview endpoint must return HTTP 200."""
        response = client.get("/api/runtime/table-preview/silver")
        assert response.status_code == 200
