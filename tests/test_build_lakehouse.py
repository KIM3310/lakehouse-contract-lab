"""Tests for lakehouse-contract-lab medallion pipeline and artifact generation.

These tests exercise the artifact-generation helpers and the API layer
without requiring a Spark/Delta runtime.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = ROOT / "scripts"


# ---------------------------------------------------------------------------
# We cannot import the full build script at module level because it depends
# on pyspark + delta.  Instead we selectively import pure-Python helpers by
# patching the heavy imports away.
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def bla():
    """Import build_lakehouse_artifacts with pyspark/delta mocked out."""
    import types

    # Create lightweight stubs for delta and pyspark so the module can load.
    delta_stub = types.ModuleType("delta")
    delta_stub.configure_spark_with_delta_pip = lambda b: b  # type: ignore[attr-defined]

    pyspark_stub = types.ModuleType("pyspark")
    pyspark_sql = types.ModuleType("pyspark.sql")
    pyspark_sql_functions = types.ModuleType("pyspark.sql.functions")
    pyspark_sql.Window = type("Window", (), {"partitionBy": staticmethod(lambda *a: None)})  # type: ignore[attr-defined]
    pyspark_sql.DataFrame = type("DataFrame", (), {})  # type: ignore[attr-defined]
    pyspark_sql.SparkSession = type("SparkSession", (), {"builder": None})  # type: ignore[attr-defined]
    pyspark_sql.functions = pyspark_sql_functions  # type: ignore[attr-defined]
    pyspark_stub.sql = pyspark_sql  # type: ignore[attr-defined]

    saved = {}
    for mod_name in ("delta", "pyspark", "pyspark.sql", "pyspark.sql.functions"):
        saved[mod_name] = sys.modules.get(mod_name)

    sys.modules["delta"] = delta_stub
    sys.modules["pyspark"] = pyspark_stub
    sys.modules["pyspark.sql"] = pyspark_sql
    sys.modules["pyspark.sql.functions"] = pyspark_sql_functions

    if str(SCRIPTS_DIR) not in sys.path:
        sys.path.insert(0, str(SCRIPTS_DIR))

    try:
        import importlib
        import build_lakehouse_artifacts as mod
        importlib.reload(mod)
        yield mod
    finally:
        for mod_name, original in saved.items():
            if original is None:
                sys.modules.pop(mod_name, None)
            else:
                sys.modules[mod_name] = original


# ---------------------------------------------------------------------------
# 1. Medallion layer execution: SOURCE_ROWS contract
# ---------------------------------------------------------------------------

class TestMedallionSourceContract:
    """Verify that SOURCE_ROWS contain the expected data for medallion processing."""

    def test_source_rows_count(self, bla) -> None:
        assert len(bla.SOURCE_ROWS) == 12

    def test_source_contains_null_customer(self, bla) -> None:
        nulls = [r for r in bla.SOURCE_ROWS if r.get("customer_id") is None]
        assert len(nulls) >= 1, "Bronze layer needs at least one null customer for quality gate testing"

    def test_source_contains_null_region(self, bla) -> None:
        nulls = [r for r in bla.SOURCE_ROWS if r.get("region") is None]
        assert len(nulls) >= 1, "Bronze layer needs at least one null region for quality gate testing"

    def test_source_contains_non_positive_amount(self, bla) -> None:
        bad = [r for r in bla.SOURCE_ROWS if (r.get("amount") or 0) <= 0]
        assert len(bad) >= 1, "Quality gate needs at least one non-positive amount"

    def test_source_contains_duplicate_order(self, bla) -> None:
        ids = [r["order_id"] for r in bla.SOURCE_ROWS]
        assert len(ids) > len(set(ids)), "Silver dedup logic needs at least one duplicate order_id"


# ---------------------------------------------------------------------------
# 2. Quality gate: review summary fallback (no OpenAI key)
# ---------------------------------------------------------------------------

class TestQualityGateAndReviewSummary:
    def test_review_summary_fallback_schema(self, bla) -> None:
        proof_pack = {
            "service": "lakehouse-contract-lab",
            "summary": {"bronzeRows": 12, "silverAcceptedRows": 8},
        }
        quality_report = {
            "summary": {"acceptedRows": 8, "failedRows": 4, "qualityPassRatePct": 66.67},
        }
        result = bla.build_review_summary_artifact(proof_pack, quality_report)
        assert result["schema"] == "lakehouse-review-summary-v1"
        assert result["service"] == "lakehouse-contract-lab"
        assert result["generationMode"] == "static-fallback"

    def test_review_summary_contains_proof_assets(self, bla) -> None:
        proof_pack = {
            "service": "lakehouse-contract-lab",
            "summary": {"bronzeRows": 12, "silverAcceptedRows": 8},
        }
        quality_report = {
            "summary": {"acceptedRows": 8, "failedRows": 4, "qualityPassRatePct": 66.67},
        }
        result = bla.build_review_summary_artifact(proof_pack, quality_report)
        assert "proofAssets" in result
        assert isinstance(result["reviewPath"], list)
        assert len(result["reviewPath"]) >= 1


# ---------------------------------------------------------------------------
# 3. SVG board generation
# ---------------------------------------------------------------------------

class TestSvgBoardGeneration:
    def _make_proof_pack(self) -> dict:
        return {
            "summary": {
                "bronzeRows": 12,
                "silverAcceptedRows": 8,
                "silverRejectedRows": 4,
                "goldRows": 4,
            },
            "governance": {
                "expectations": [
                    {"name": "customer_present", "passed": 11, "failed": 1},
                    {"name": "positive_amount", "passed": 11, "failed": 1},
                ],
            },
        }

    def test_build_svg_creates_file(self, bla, tmp_path: Path, monkeypatch) -> None:
        monkeypatch.setattr(bla, "DOCS_DIR", tmp_path)
        bla.build_svg(self._make_proof_pack())
        svg_path = tmp_path / "lakehouse-contract-board.svg"
        assert svg_path.exists()
        content = svg_path.read_text()
        assert content.startswith("<?xml")
        assert "</svg>" in content

    def test_svg_contains_layer_labels(self, bla, tmp_path: Path, monkeypatch) -> None:
        monkeypatch.setattr(bla, "DOCS_DIR", tmp_path)
        bla.build_svg(self._make_proof_pack())
        content = (tmp_path / "lakehouse-contract-board.svg").read_text()
        for label in ("Bronze", "Silver", "Gold"):
            assert label in content


# ---------------------------------------------------------------------------
# 4. Contract surface schema (API layer)
# ---------------------------------------------------------------------------

class TestContractSurfaceApi:
    """Verify the FastAPI endpoints return the expected schema markers."""

    @pytest.fixture(autouse=True)
    def _setup_artifacts(self, bla, tmp_path: Path, monkeypatch) -> None:
        """Write minimal artifact JSON files so the API can load them."""
        artifacts = tmp_path / "artifacts"
        artifacts.mkdir()
        proof = {
            "service": "lakehouse-contract-lab",
            "status": "ok",
            "generatedAt": "2026-03-14T00:00:00+00:00",
            "schema": "lakehouse-proof-pack-v1",
            "summary": {
                "sourceRows": 12, "bronzeRows": 12, "silverAcceptedRows": 8,
                "silverRejectedRows": 4, "goldRows": 4, "deltaTables": 3,
                "qualityPassRatePct": 66.67,
            },
            "tables": [{"layer": "gold", "tableName": "gold_region_kpis", "deltaVersion": 0, "rows": 4, "contract": []}],
            "governance": {
                "approvalBoundary": "test",
                "expectations": [{"name": "positive_amount", "passed": 11, "failed": 1, "rule": "test", "layer": "silver"}],
                "rejectedReasons": [],
            },
            "snowflakeFit": {"whyItMatters": "", "reviewPath": []},
            "databricksFit": {"whyItMatters": "", "reviewPath": []},
            "proofAssets": [],
            "links": {},
        }
        quality = {"schema": "lakehouse-quality-report-v1", "summary": {}, "expectations": [], "rejectedPreview": []}
        review = {"schema": "lakehouse-review-summary-v1"}
        gold_preview = {"layer": "gold", "rows": [{"region": "KR-SEOUL"}]}

        for name, data in [
            ("lakehouse-proof-pack.json", proof),
            ("quality-report.json", quality),
            ("review-summary.json", review),
            ("gold-preview.json", gold_preview),
        ]:
            (artifacts / name).write_text(json.dumps(data))

        # Redirect the app's ARTIFACTS_DIR to our temp location
        import app.main as app_main
        monkeypatch.setattr(app_main, "ARTIFACTS_DIR", artifacts)

    def test_health_returns_proof_links(self) -> None:
        from fastapi.testclient import TestClient
        from app.main import app

        client = TestClient(app)
        resp = client.get("/health")
        assert resp.status_code == 200
        payload = resp.json()
        assert payload["ok"] is True
        assert "proofPack" in payload["links"]


# ---------------------------------------------------------------------------
# 5. Utility helpers
# ---------------------------------------------------------------------------

class TestUtilities:
    def test_normalize_value_datetime(self, bla) -> None:
        dt = datetime(2026, 3, 14, 9, 0, 0, tzinfo=timezone.utc)
        assert bla.normalize_value(dt) == dt.isoformat()

    def test_normalize_value_decimal(self, bla) -> None:
        assert bla.normalize_value(Decimal("3.14")) == 3.14

    def test_write_json_creates_nested_dirs(self, bla, tmp_path: Path) -> None:
        target = tmp_path / "sub" / "test.json"
        bla.write_json(target, {"key": "value"})
        assert target.exists()
        assert json.loads(target.read_text()) == {"key": "value"}
