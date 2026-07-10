"""Pipeline and artifact generation tests (no Spark runtime needed)."""

from __future__ import annotations

import json
import sys
import threading
import types
import urllib.error
import urllib.request
from datetime import datetime, timezone
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pytest

ROOT: Path = Path(__file__).resolve().parents[1]
SCRIPTS_DIR: Path = ROOT / "scripts"


@pytest.fixture(scope="module")
def bla():
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


class TestMedallionSourceContract:
    def test_source_rows_count(self, bla) -> None:
        assert len(bla.SOURCE_ROWS) == 12

    def test_source_contains_null_customer(self, bla) -> None:
        nulls = [r for r in bla.SOURCE_ROWS if r.get("customer_id") is None]
        assert len(nulls) >= 1

    def test_source_contains_null_region(self, bla) -> None:
        nulls = [r for r in bla.SOURCE_ROWS if r.get("region") is None]
        assert len(nulls) >= 1

    def test_source_contains_non_positive_amount(self, bla) -> None:
        bad = [r for r in bla.SOURCE_ROWS if (r.get("amount") or 0) <= 0]
        assert len(bad) >= 1

    def test_source_contains_duplicate_order(self, bla) -> None:
        ids = [r["order_id"] for r in bla.SOURCE_ROWS]
        assert len(ids) > len(set(ids))

    def test_source_rows_have_required_fields(self, bla) -> None:
        required = {"order_id", "customer_id", "region", "channel", "status", "amount", "currency", "order_ts"}
        for row in bla.SOURCE_ROWS:
            assert required.issubset(row.keys()), f"Row missing fields: {required - row.keys()}"

    def test_source_rows_all_usd_currency(self, bla) -> None:
        currencies = {r["currency"] for r in bla.SOURCE_ROWS}
        assert currencies == {"USD"}

    def test_source_rows_order_ids_prefixed(self, bla) -> None:
        for row in bla.SOURCE_ROWS:
            assert row["order_id"].startswith("O-"), f"Unexpected order_id format: {row['order_id']}"


class TestQualityGateAndArchitectureSummary:
    def _make_inputs(self) -> tuple[dict, dict]:
        proof_pack = {
            "service": "lakehouse-contract-lab",
            "summary": {"bronzeRows": 12, "silverAcceptedRows": 8},
        }
        quality_report = {
            "summary": {"acceptedRows": 8, "failedRows": 4, "qualityPassRatePct": 66.67},
        }
        return proof_pack, quality_report

    def test_architecture_summary_fallback_schema(self, bla) -> None:
        proof_pack, quality_report = self._make_inputs()
        result = bla.build_architecture_summary_artifact(proof_pack, quality_report)
        assert result["schema"] == "lakehouse-architecture-summary-v1"
        assert result["service"] == "lakehouse-contract-lab"
        assert result["generationMode"] == "static-fallback"

    def test_architecture_summary_contains_proof_assets(self, bla) -> None:
        proof_pack, quality_report = self._make_inputs()
        result = bla.build_architecture_summary_artifact(proof_pack, quality_report)
        assert "proofAssets" in result
        assert isinstance(result["architecturePath"], list)
        assert len(result["architecturePath"]) >= 1

    def test_architecture_summary_has_headline(self, bla) -> None:
        proof_pack, quality_report = self._make_inputs()
        result = bla.build_architecture_summary_artifact(proof_pack, quality_report)
        assert isinstance(result["headline"], str)
        assert len(result["headline"]) > 0

    def test_architecture_summary_has_generated_at(self, bla) -> None:
        proof_pack, quality_report = self._make_inputs()
        result = bla.build_architecture_summary_artifact(proof_pack, quality_report)
        assert "generatedAt" in result
        datetime.fromisoformat(result["generatedAt"])

    def test_architecture_summary_fallback_summary_keys(self, bla) -> None:
        proof_pack, quality_report = self._make_inputs()
        result = bla.build_architecture_summary_artifact(proof_pack, quality_report)
        expected_keys = {"platformFit", "qualityPosture", "handoffPosture", "nextAction"}
        assert expected_keys == set(result["summary"].keys())


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

    def test_svg_contains_quality_gate_names(self, bla, tmp_path: Path, monkeypatch) -> None:
        monkeypatch.setattr(bla, "DOCS_DIR", tmp_path)
        bla.build_svg(self._make_proof_pack())
        content = (tmp_path / "lakehouse-contract-board.svg").read_text()
        assert "customer_present" in content
        assert "positive_amount" in content

    def test_svg_contains_row_counts(self, bla, tmp_path: Path, monkeypatch) -> None:
        monkeypatch.setattr(bla, "DOCS_DIR", tmp_path)
        bla.build_svg(self._make_proof_pack())
        content = (tmp_path / "lakehouse-contract-board.svg").read_text()
        assert "12 rows" in content
        assert "8 accepted" in content
        assert "4 KPI rows" in content


class TestContractSurfaceApi:
    @pytest.fixture(autouse=True)
    def _setup_artifacts(self, bla, tmp_path: Path, monkeypatch) -> None:
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
            "snowflakeFit": {"whyItMatters": "", "architecturePath": []},
            "databricksFit": {"whyItMatters": "", "architecturePath": []},
            "proofAssets": [],
            "links": {},
        }
        quality = {
            "schema": "lakehouse-quality-report-v1",
            "summary": {},
            "expectations": [],
            "rejectedPreview": [],
        }
        architecture = {"schema": "lakehouse-architecture-summary-v1"}
        gold_prev = {"layer": "gold", "rows": [{"region": "KR-SEOUL"}]}
        bronze_prev = {"layer": "bronze", "rows": [{"order_id": "O-1001"}]}
        silver_prev = {"layer": "silver", "rows": [{"order_id": "O-1001"}]}

        for name, data in [
            ("lakehouse-proof-pack.json", proof),
            ("quality-report.json", quality),
            ("architecture-summary.json", architecture),
            ("gold-preview.json", gold_prev),
            ("bronze-preview.json", bronze_prev),
            ("silver-preview.json", silver_prev),
        ]:
            (artifacts / name).write_text(json.dumps(data))

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
        assert "links" in payload

    def test_health_contains_openai_refresh(self) -> None:
        from fastapi.testclient import TestClient

        from app.main import app

        client = TestClient(app)
        resp = client.get("/health")
        payload = resp.json()
        assert "openai_refresh" in payload
        assert payload["openai_refresh"]["deploymentMode"] == "artifact-refresh-only"

    def test_table_preview_unknown_layer_404(self) -> None:
        from fastapi.testclient import TestClient

        from app.main import app

        client = TestClient(app)
        resp = client.get("/api/runtime/table-preview/platinum")
        assert resp.status_code == 404


class TestUtilities:
    def test_normalize_value_datetime(self, bla) -> None:
        dt = datetime(2026, 3, 14, 9, 0, 0, tzinfo=timezone.utc)
        assert bla.normalize_value(dt) == dt.isoformat()

    def test_normalize_value_decimal(self, bla) -> None:
        assert bla.normalize_value(Decimal("3.14")) == 3.14

    def test_normalize_value_passthrough(self, bla) -> None:
        assert bla.normalize_value("hello") == "hello"
        assert bla.normalize_value(42) == 42
        assert bla.normalize_value(None) is None

    def test_write_json_creates_nested_dirs(self, bla, tmp_path: Path) -> None:
        target = tmp_path / "sub" / "test.json"
        bla.write_json(target, {"key": "value"})
        assert target.exists()
        assert json.loads(target.read_text()) == {"key": "value"}

    def test_write_json_valid_utf8(self, bla, tmp_path: Path) -> None:
        target = tmp_path / "unicode.json"
        bla.write_json(target, {"name": "test"})
        content = target.read_text(encoding="utf-8")
        parsed = json.loads(content)
        assert parsed["name"] == "test"

    def test_latest_delta_version_empty(self, bla, tmp_path: Path) -> None:
        empty_table = tmp_path / "empty_table"
        log_dir = empty_table / "_delta_log"
        log_dir.mkdir(parents=True)
        assert bla.latest_delta_version(empty_table) is None

    def test_latest_delta_version_with_entries(self, bla, tmp_path: Path) -> None:
        table = tmp_path / "test_table"
        log_dir = table / "_delta_log"
        log_dir.mkdir(parents=True)
        (log_dir / "00000000000000000000.json").write_text("{}")
        (log_dir / "00000000000000000001.json").write_text("{}")
        (log_dir / "00000000000000000002.json").write_text("{}")
        assert bla.latest_delta_version(table) == 2


class TestOpenAIRefreshUrlSecurity:
    def test_default_openrouter_url_is_allowed(self, bla, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("OPENROUTER_API_KEY", "token")

        assert bla.build_openai_chat_completions_url() == "https://openrouter.ai/api/v1/chat/completions"

    def test_rejects_insecure_authorization_host(self, bla, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("OPENAI_API_KEY", "token")
        monkeypatch.setenv("OPENAI_BASE_URL", "http://api.openai.com/v1")

        with pytest.raises(ValueError, match="HTTPS"):
            bla.build_openai_chat_completions_url()

    def test_custom_authorization_host_requires_explicit_opt_in(self, bla, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("OPENAI_API_KEY", "token")
        monkeypatch.setenv("OPENAI_BASE_URL", "https://llm-gateway.internal/v1")

        with pytest.raises(ValueError, match="ALLOW_CUSTOM_OPENAI_HOST"):
            bla.build_openai_chat_completions_url()

    def test_custom_authorization_host_opt_in_allows_https_host(self, bla, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("OPENAI_API_KEY", "token")
        monkeypatch.setenv("OPENAI_BASE_URL", "https://llm-gateway.internal/v1")
        monkeypatch.setenv("ALLOW_CUSTOM_OPENAI_HOST", "true")

        assert bla.build_openai_chat_completions_url() == "https://llm-gateway.internal/v1/chat/completions"

    def test_authorization_request_does_not_follow_cross_origin_redirect(self, bla) -> None:
        target_requests: list[str | None] = []

        class TargetHandler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                target_requests.append(self.headers.get("Authorization"))
                self.send_response(200)
                self.end_headers()

            def log_message(self, *_args: object) -> None:
                return None

        target = HTTPServer(("127.0.0.1", 0), TargetHandler)
        target_thread = threading.Thread(target=target.serve_forever, daemon=True)
        target_thread.start()

        class RedirectHandler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                self.send_response(302)
                self.send_header("Location", f"http://127.0.0.1:{target.server_port}/leak")
                self.end_headers()

            def log_message(self, *_args: object) -> None:
                return None

        redirector = HTTPServer(("127.0.0.1", 0), RedirectHandler)
        redirect_thread = threading.Thread(target=redirector.serve_forever, daemon=True)
        redirect_thread.start()
        try:
            request = urllib.request.Request(
                f"http://127.0.0.1:{redirector.server_port}/chat/completions",
                data=b"{}",
                headers={"Authorization": "Bearer secret-token"},
                method="POST",
            )

            with pytest.raises(urllib.error.HTTPError) as exc_info:
                bla.open_no_redirect(request, timeout=2)

            assert exc_info.value.code == 302
            assert target_requests == []
        finally:
            redirector.shutdown()
            redirector.server_close()
            target.shutdown()
            target.server_close()
