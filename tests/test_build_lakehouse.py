"""Tests for lakehouse-contract-lab medallion pipeline and artifact generation.

These tests exercise the artifact-generation helpers and the API layer
without requiring a Spark/Delta runtime.
"""

from __future__ import annotations

import json
import sys
import types
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

ROOT: Path = Path(__file__).resolve().parents[1]
SCRIPTS_DIR: Path = ROOT / "scripts"


# ---------------------------------------------------------------------------
# We cannot import the full build script at module level because it depends
# on pyspark + delta.  Instead we selectively import pure-Python helpers by
# patching the heavy imports away.
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def bla():
    """Import build_lakehouse_artifacts with pyspark/delta mocked out."""
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

    saved: dict[str, types.ModuleType | None] = {}
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
        """Source data must contain exactly 12 rows for the pipeline."""
        assert len(bla.SOURCE_ROWS) == 12

    def test_source_contains_null_customer(self, bla) -> None:
        """Bronze layer needs at least one null customer for quality gate testing."""
        nulls = [r for r in bla.SOURCE_ROWS if r.get("customer_id") is None]
        assert len(nulls) >= 1

    def test_source_contains_null_region(self, bla) -> None:
        """Bronze layer needs at least one null region for quality gate testing."""
        nulls = [r for r in bla.SOURCE_ROWS if r.get("region") is None]
        assert len(nulls) >= 1

    def test_source_contains_non_positive_amount(self, bla) -> None:
        """Quality gate needs at least one non-positive amount to exercise rejection."""
        bad = [r for r in bla.SOURCE_ROWS if (r.get("amount") or 0) <= 0]
        assert len(bad) >= 1

    def test_source_contains_duplicate_order(self, bla) -> None:
        """Silver dedup logic needs at least one duplicate order_id."""
        ids = [r["order_id"] for r in bla.SOURCE_ROWS]
        assert len(ids) > len(set(ids))

    def test_source_rows_have_required_fields(self, bla) -> None:
        """Every source row must include the mandatory field set."""
        required = {"order_id", "customer_id", "region", "channel", "status", "amount", "currency", "order_ts"}
        for row in bla.SOURCE_ROWS:
            assert required.issubset(row.keys()), f"Row missing fields: {required - row.keys()}"

    def test_source_rows_all_usd_currency(self, bla) -> None:
        """All source rows should use USD as the currency."""
        currencies = {r["currency"] for r in bla.SOURCE_ROWS}
        assert currencies == {"USD"}

    def test_source_rows_order_ids_prefixed(self, bla) -> None:
        """All order_id values should follow the O-NNNN naming convention."""
        for row in bla.SOURCE_ROWS:
            assert row["order_id"].startswith("O-"), f"Unexpected order_id format: {row['order_id']}"


# ---------------------------------------------------------------------------
# 2. Quality gate: review summary fallback (no OpenAI key)
# ---------------------------------------------------------------------------


class TestQualityGateAndReviewSummary:
    """Validate the review summary artifact under static-fallback mode."""

    def _make_inputs(self) -> tuple[dict, dict]:
        """Return minimal proof_pack and quality_report for testing."""
        proof_pack = {
            "service": "lakehouse-contract-lab",
            "summary": {"bronzeRows": 12, "silverAcceptedRows": 8},
        }
        quality_report = {
            "summary": {"acceptedRows": 8, "failedRows": 4, "qualityPassRatePct": 66.67},
        }
        return proof_pack, quality_report

    def test_review_summary_fallback_schema(self, bla) -> None:
        """Fallback summary must use the correct schema and generation mode."""
        proof_pack, quality_report = self._make_inputs()
        result = bla.build_review_summary_artifact(proof_pack, quality_report)
        assert result["schema"] == "lakehouse-review-summary-v1"
        assert result["service"] == "lakehouse-contract-lab"
        assert result["generationMode"] == "static-fallback"

    def test_review_summary_contains_proof_assets(self, bla) -> None:
        """Fallback summary must include proof assets and review paths."""
        proof_pack, quality_report = self._make_inputs()
        result = bla.build_review_summary_artifact(proof_pack, quality_report)
        assert "proofAssets" in result
        assert isinstance(result["reviewPath"], list)
        assert len(result["reviewPath"]) >= 1

    def test_review_summary_has_headline(self, bla) -> None:
        """Fallback summary must include a non-empty headline."""
        proof_pack, quality_report = self._make_inputs()
        result = bla.build_review_summary_artifact(proof_pack, quality_report)
        assert isinstance(result["headline"], str)
        assert len(result["headline"]) > 0

    def test_review_summary_has_generated_at(self, bla) -> None:
        """Fallback summary must include a parseable generatedAt timestamp."""
        proof_pack, quality_report = self._make_inputs()
        result = bla.build_review_summary_artifact(proof_pack, quality_report)
        assert "generatedAt" in result
        # Should be parseable as an ISO timestamp
        datetime.fromisoformat(result["generatedAt"])

    def test_review_summary_fallback_summary_keys(self, bla) -> None:
        """Fallback summary.summary must contain all required posture keys."""
        proof_pack, quality_report = self._make_inputs()
        result = bla.build_review_summary_artifact(proof_pack, quality_report)
        expected_keys = {"platformFit", "qualityPosture", "handoffPosture", "nextAction"}
        assert expected_keys == set(result["summary"].keys())


# ---------------------------------------------------------------------------
# 3. SVG board generation
# ---------------------------------------------------------------------------


class TestSvgBoardGeneration:
    """Validate the SVG architecture board output."""

    def _make_proof_pack(self) -> dict:
        """Return a minimal proof_pack with summary and governance for SVG generation."""
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
        """SVG generation must create the board file on disk."""
        monkeypatch.setattr(bla, "DOCS_DIR", tmp_path)
        bla.build_svg(self._make_proof_pack())
        svg_path = tmp_path / "lakehouse-contract-board.svg"
        assert svg_path.exists()
        content = svg_path.read_text()
        assert content.startswith("<?xml")
        assert "</svg>" in content

    def test_svg_contains_layer_labels(self, bla, tmp_path: Path, monkeypatch) -> None:
        """SVG must reference all three medallion layer names."""
        monkeypatch.setattr(bla, "DOCS_DIR", tmp_path)
        bla.build_svg(self._make_proof_pack())
        content = (tmp_path / "lakehouse-contract-board.svg").read_text()
        for label in ("Bronze", "Silver", "Gold"):
            assert label in content

    def test_svg_contains_quality_gate_names(self, bla, tmp_path: Path, monkeypatch) -> None:
        """SVG must render all quality gate expectation names."""
        monkeypatch.setattr(bla, "DOCS_DIR", tmp_path)
        bla.build_svg(self._make_proof_pack())
        content = (tmp_path / "lakehouse-contract-board.svg").read_text()
        assert "customer_present" in content
        assert "positive_amount" in content

    def test_svg_contains_row_counts(self, bla, tmp_path: Path, monkeypatch) -> None:
        """SVG must display the actual row counts from the pipeline output."""
        monkeypatch.setattr(bla, "DOCS_DIR", tmp_path)
        bla.build_svg(self._make_proof_pack())
        content = (tmp_path / "lakehouse-contract-board.svg").read_text()
        assert "12 rows" in content
        assert "8 accepted" in content
        assert "4 KPI rows" in content


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
                "sourceRows": 12,
                "bronzeRows": 12,
                "silverAcceptedRows": 8,
                "silverRejectedRows": 4,
                "goldRows": 4,
                "deltaTables": 3,
                "qualityPassRatePct": 66.67,
            },
            "tables": [
                {
                    "layer": "gold",
                    "tableName": "gold_region_kpis",
                    "deltaVersion": 0,
                    "rows": 4,
                    "contract": [],
                }
            ],
            "governance": {
                "approvalBoundary": "test",
                "expectations": [
                    {
                        "name": "positive_amount",
                        "passed": 11,
                        "failed": 1,
                        "rule": "test",
                        "layer": "silver",
                    }
                ],
                "rejectedReasons": [],
            },
            "snowflakeFit": {"whyItMatters": "", "reviewPath": []},
            "databricksFit": {"whyItMatters": "", "reviewPath": []},
            "proofAssets": [],
            "links": {},
        }
        quality = {
            "schema": "lakehouse-quality-report-v1",
            "summary": {},
            "expectations": [],
            "rejectedPreview": [],
        }
        review = {"schema": "lakehouse-review-summary-v1"}
        gold_prev = {"layer": "gold", "rows": [{"region": "KR-SEOUL"}]}
        bronze_prev = {"layer": "bronze", "rows": [{"order_id": "O-1001"}]}
        silver_prev = {"layer": "silver", "rows": [{"order_id": "O-1001"}]}

        for name, data in [
            ("lakehouse-proof-pack.json", proof),
            ("quality-report.json", quality),
            ("review-summary.json", review),
            ("gold-preview.json", gold_prev),
            ("bronze-preview.json", bronze_prev),
            ("silver-preview.json", silver_prev),
        ]:
            (artifacts / name).write_text(json.dumps(data))

        # Redirect the app's ARTIFACTS_DIR to our temp location
        import app.main as app_main

        monkeypatch.setattr(app_main, "ARTIFACTS_DIR", artifacts)

    def test_health_returns_proof_links(self) -> None:
        """Health endpoint must return ok=True with proof-pack links."""
        from fastapi.testclient import TestClient

        from app.main import app

        client = TestClient(app)
        resp = client.get("/health")
        assert resp.status_code == 200
        payload = resp.json()
        assert payload["ok"] is True
        assert "proofPack" in payload["links"]

    def test_health_contains_openai_refresh(self) -> None:
        """Health endpoint must include the openai_refresh contract block."""
        from fastapi.testclient import TestClient

        from app.main import app

        client = TestClient(app)
        resp = client.get("/health")
        payload = resp.json()
        assert "openai_refresh" in payload
        assert payload["openai_refresh"]["deploymentMode"] == "artifact-refresh-only"

    def test_table_preview_unknown_layer_404(self) -> None:
        """Requesting an unknown layer must return HTTP 404."""
        from fastapi.testclient import TestClient

        from app.main import app

        client = TestClient(app)
        resp = client.get("/api/runtime/table-preview/platinum")
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# 5. Utility helpers
# ---------------------------------------------------------------------------


class TestUtilities:
    """Unit tests for pure-Python helper functions."""

    def test_normalize_value_datetime(self, bla) -> None:
        """datetime values should be converted to ISO-8601 strings."""
        dt = datetime(2026, 3, 14, 9, 0, 0, tzinfo=timezone.utc)
        assert bla.normalize_value(dt) == dt.isoformat()

    def test_normalize_value_decimal(self, bla) -> None:
        """Decimal values should be converted to float."""
        assert bla.normalize_value(Decimal("3.14")) == 3.14

    def test_normalize_value_passthrough(self, bla) -> None:
        """Plain strings and ints should pass through unchanged."""
        assert bla.normalize_value("hello") == "hello"
        assert bla.normalize_value(42) == 42
        assert bla.normalize_value(None) is None

    def test_write_json_creates_nested_dirs(self, bla, tmp_path: Path) -> None:
        """write_json must create intermediate directories automatically."""
        target = tmp_path / "sub" / "test.json"
        bla.write_json(target, {"key": "value"})
        assert target.exists()
        assert json.loads(target.read_text()) == {"key": "value"}

    def test_write_json_valid_utf8(self, bla, tmp_path: Path) -> None:
        """write_json output must be valid UTF-8 encoded JSON."""
        target = tmp_path / "unicode.json"
        bla.write_json(target, {"name": "test"})
        content = target.read_text(encoding="utf-8")
        parsed = json.loads(content)
        assert parsed["name"] == "test"

    def test_latest_delta_version_empty(self, bla, tmp_path: Path) -> None:
        """latest_delta_version returns None when no Delta log exists."""
        empty_table = tmp_path / "empty_table"
        log_dir = empty_table / "_delta_log"
        log_dir.mkdir(parents=True)
        assert bla.latest_delta_version(empty_table) is None

    def test_latest_delta_version_with_entries(self, bla, tmp_path: Path) -> None:
        """latest_delta_version returns the highest version number."""
        table = tmp_path / "test_table"
        log_dir = table / "_delta_log"
        log_dir.mkdir(parents=True)
        (log_dir / "00000000000000000000.json").write_text("{}")
        (log_dir / "00000000000000000001.json").write_text("{}")
        (log_dir / "00000000000000000002.json").write_text("{}")
        assert bla.latest_delta_version(table) == 2
