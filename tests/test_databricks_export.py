"""Mocked tests for the Databricks Unity Catalog export adapter.

These tests exercise the Databricks adapter's workspace client construction,
schema creation, Delta table creation, and MERGE upsert logic without
requiring a live Databricks workspace. All SDK calls are mocked.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from app.databricks_adapter import (
    _build_create_table_sql,
    _build_merge_sql,
    _get_full_table_name,
    _quote,
    _state_value,
    _statement_error_message,
    export_gold_kpis_to_databricks,
    is_configured,
    query_region_kpis,
)

# ---------------------------------------------------------------------------
# Sample data
# ---------------------------------------------------------------------------

SAMPLE_GOLD_ROWS: list[dict[str, Any]] = [
    {
        "region": "KR-SEOUL",
        "gross_revenue_usd": 440.90,
        "accepted_orders": 2,
        "completed_orders": 1,
        "pipeline_orders": 1,
        "distinct_customers": 2,
    },
    {
        "region": "US-WEST",
        "gross_revenue_usd": 510.35,
        "accepted_orders": 3,
        "completed_orders": 2,
        "pipeline_orders": 1,
        "distinct_customers": 3,
    },
]


# ---------------------------------------------------------------------------
# Configuration tests
# ---------------------------------------------------------------------------


class TestDatabricksConfiguration:
    """Test env-var gating and settings resolution."""

    def test_is_configured_returns_false_when_host_missing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Adapter must report unconfigured when DATABRICKS_HOST is empty."""
        monkeypatch.setenv("DATABRICKS_HOST", "")
        monkeypatch.setenv("DATABRICKS_TOKEN", "")
        monkeypatch.setenv("DATABRICKS_CONFIG_PROFILE", "")
        monkeypatch.setenv("DATABRICKS_AUTH_TYPE", "")
        monkeypatch.setenv("DATABRICKS_CLIENT_ID", "")
        monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "")
        assert is_configured() is False

    def test_is_configured_returns_true_with_token(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Adapter must report configured when host and token are set."""
        monkeypatch.setenv("DATABRICKS_HOST", "https://adb-123.azuredatabricks.net")
        monkeypatch.setenv("DATABRICKS_TOKEN", "dapi-test-token")
        monkeypatch.setenv("DATABRICKS_CONFIG_PROFILE", "")
        monkeypatch.setenv("DATABRICKS_AUTH_TYPE", "")
        monkeypatch.setenv("DATABRICKS_CLIENT_ID", "")
        monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "")
        assert is_configured() is True

    def test_is_configured_returns_true_with_profile(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Adapter must report configured when host and CLI profile are set."""
        monkeypatch.setenv("DATABRICKS_HOST", "https://adb-123.azuredatabricks.net")
        monkeypatch.setenv("DATABRICKS_TOKEN", "")
        monkeypatch.setenv("DATABRICKS_CONFIG_PROFILE", "DEFAULT")
        monkeypatch.setenv("DATABRICKS_AUTH_TYPE", "")
        monkeypatch.setenv("DATABRICKS_CLIENT_ID", "")
        monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "")
        assert is_configured() is True

    def test_is_configured_returns_true_with_service_principal(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Adapter must report configured when host and client_id+secret are set."""
        monkeypatch.setenv("DATABRICKS_HOST", "https://adb-123.azuredatabricks.net")
        monkeypatch.setenv("DATABRICKS_TOKEN", "")
        monkeypatch.setenv("DATABRICKS_CONFIG_PROFILE", "")
        monkeypatch.setenv("DATABRICKS_AUTH_TYPE", "")
        monkeypatch.setenv("DATABRICKS_CLIENT_ID", "app-id")
        monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "app-secret")
        assert is_configured() is True


# ---------------------------------------------------------------------------
# SQL generation tests
# ---------------------------------------------------------------------------


class TestDatabricksSqlGeneration:
    """Test SQL statement generation for DDL and MERGE."""

    def test_quote_handles_plain_name(self) -> None:
        """_quote must backtick-wrap a plain identifier."""
        assert _quote("main") == "`main`"

    def test_quote_escapes_backticks(self) -> None:
        """_quote must double any backticks inside the identifier."""
        assert _quote("my`catalog") == "`my``catalog`"

    def test_get_full_table_name(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """_get_full_table_name must return catalog.schema.table in backtick form."""
        monkeypatch.setenv("DATABRICKS_CATALOG", "workspace")
        monkeypatch.setenv("DATABRICKS_SCHEMA", "lakehouse_lab")
        full_name = _get_full_table_name()
        assert full_name == "`workspace`.`lakehouse_lab`.`region_kpis`"

    def test_build_create_table_sql_contains_delta(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """CREATE TABLE SQL must specify USING DELTA and include all KPI columns."""
        monkeypatch.setenv("DATABRICKS_CATALOG", "main")
        monkeypatch.setenv("DATABRICKS_SCHEMA", "lakehouse_lab")
        sql = _build_create_table_sql()
        assert "CREATE TABLE IF NOT EXISTS" in sql
        assert "USING DELTA" in sql
        assert "region" in sql
        assert "gross_revenue_usd" in sql
        assert "accepted_orders" in sql
        assert "completed_orders" in sql
        assert "pipeline_orders" in sql
        assert "distinct_customers" in sql
        assert "loaded_at" in sql
        assert "delta.autoOptimize.optimizeWrite" in sql

    def test_build_merge_sql_structure(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """MERGE SQL must include USING VALUES, WHEN MATCHED, and WHEN NOT MATCHED."""
        monkeypatch.setenv("DATABRICKS_CATALOG", "main")
        monkeypatch.setenv("DATABRICKS_SCHEMA", "lakehouse_lab")
        sql = _build_merge_sql(SAMPLE_GOLD_ROWS)
        assert "MERGE INTO" in sql
        assert "target.region = source.region" in sql
        assert "WHEN MATCHED THEN UPDATE" in sql
        assert "WHEN NOT MATCHED THEN INSERT" in sql
        assert "KR-SEOUL" in sql
        assert "US-WEST" in sql

    def test_build_merge_sql_escapes_single_quotes(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """MERGE SQL must escape single quotes in region names."""
        monkeypatch.setenv("DATABRICKS_CATALOG", "main")
        monkeypatch.setenv("DATABRICKS_SCHEMA", "lakehouse_lab")
        rows = [
            {
                "region": "O'Brien County",
                "gross_revenue_usd": 100.0,
                "accepted_orders": 1,
                "completed_orders": 1,
                "pipeline_orders": 0,
                "distinct_customers": 1,
            }
        ]
        sql = _build_merge_sql(rows)
        assert "O''Brien County" in sql


# ---------------------------------------------------------------------------
# Response parsing helpers
# ---------------------------------------------------------------------------


class TestDatabricksResponseHelpers:
    """Test response parsing utility functions."""

    def test_state_value_extracts_succeeded(self) -> None:
        """_state_value must extract the state string from a response object."""
        response = MagicMock()
        response.status.state.value = "SUCCEEDED"
        assert _state_value(response) == "SUCCEEDED"

    def test_state_value_handles_none_status(self) -> None:
        """_state_value must return empty string when status is None."""
        response = MagicMock()
        response.status = None
        result = _state_value(response)
        assert result == ""

    def test_statement_error_message_extracts_message(self) -> None:
        """_statement_error_message must extract error message from response."""
        response = MagicMock()
        response.status.error.message = "Table not found"
        assert _statement_error_message(response) == "Table not found"

    def test_statement_error_message_handles_no_error(self) -> None:
        """_statement_error_message must return fallback when error is None."""
        response = MagicMock()
        response.status.error = None
        result = _statement_error_message(response)
        assert "Unknown" in result


# ---------------------------------------------------------------------------
# End-to-end export function tests
# ---------------------------------------------------------------------------


class TestDatabricksExportFunction:
    """Test the public export_gold_kpis_to_databricks function."""

    def test_export_skips_when_not_configured(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Export must return False when DATABRICKS_HOST is not set."""
        monkeypatch.setenv("DATABRICKS_HOST", "")
        monkeypatch.setenv("DATABRICKS_TOKEN", "")
        monkeypatch.setenv("DATABRICKS_CONFIG_PROFILE", "")
        monkeypatch.setenv("DATABRICKS_AUTH_TYPE", "")
        monkeypatch.setenv("DATABRICKS_CLIENT_ID", "")
        monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "")
        result = export_gold_kpis_to_databricks(SAMPLE_GOLD_ROWS)
        assert result is False

    def test_export_returns_false_for_empty_rows(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Export must return False when given an empty row list."""
        monkeypatch.setenv("DATABRICKS_HOST", "https://adb-123.azuredatabricks.net")
        monkeypatch.setenv("DATABRICKS_TOKEN", "dapi-test")
        monkeypatch.setenv("DATABRICKS_CONFIG_PROFILE", "")
        monkeypatch.setenv("DATABRICKS_AUTH_TYPE", "")
        monkeypatch.setenv("DATABRICKS_CLIENT_ID", "")
        monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "")
        result = export_gold_kpis_to_databricks([])
        assert result is False

    @patch("app.databricks_adapter._build_workspace_client")
    def test_export_success_with_mocked_client(
        self, mock_build_client: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Export must call schema setup, table creation, and MERGE when configured."""
        monkeypatch.setenv("DATABRICKS_HOST", "https://adb-123.azuredatabricks.net")
        monkeypatch.setenv("DATABRICKS_TOKEN", "dapi-test")
        monkeypatch.setenv("DATABRICKS_CONFIG_PROFILE", "")
        monkeypatch.setenv("DATABRICKS_AUTH_TYPE", "")
        monkeypatch.setenv("DATABRICKS_CLIENT_ID", "")
        monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "")
        monkeypatch.setenv("DATABRICKS_CATALOG", "main")
        monkeypatch.setenv("DATABRICKS_SCHEMA", "lakehouse_lab")
        monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "wh-123")

        mock_client = MagicMock()
        mock_build_client.return_value = mock_client

        # Mock successful statement execution
        mock_response = MagicMock()
        mock_response.status.state.value = "SUCCEEDED"
        mock_client.statement_execution.execute_statement.return_value = mock_response

        result = export_gold_kpis_to_databricks(SAMPLE_GOLD_ROWS)
        assert result is True

        # Should have been called for: CREATE CATALOG, CREATE SCHEMA, CREATE TABLE, MERGE
        assert mock_client.statement_execution.execute_statement.call_count == 4

    @patch("app.databricks_adapter._build_workspace_client")
    def test_export_returns_false_on_statement_failure(
        self, mock_build_client: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Export must return False when a SQL statement execution fails."""
        monkeypatch.setenv("DATABRICKS_HOST", "https://adb-123.azuredatabricks.net")
        monkeypatch.setenv("DATABRICKS_TOKEN", "dapi-test")
        monkeypatch.setenv("DATABRICKS_CONFIG_PROFILE", "")
        monkeypatch.setenv("DATABRICKS_AUTH_TYPE", "")
        monkeypatch.setenv("DATABRICKS_CLIENT_ID", "")
        monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "")
        monkeypatch.setenv("DATABRICKS_CATALOG", "main")
        monkeypatch.setenv("DATABRICKS_SCHEMA", "lakehouse_lab")
        monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "wh-123")

        mock_client = MagicMock()
        mock_build_client.return_value = mock_client

        # Mock failed statement execution
        mock_response = MagicMock()
        mock_response.status.state.value = "FAILED"
        mock_response.status.error.message = "Access denied"
        mock_client.statement_execution.execute_statement.return_value = mock_response

        result = export_gold_kpis_to_databricks(SAMPLE_GOLD_ROWS)
        assert result is False


# ---------------------------------------------------------------------------
# Unity Catalog table creation tests
# ---------------------------------------------------------------------------


class TestDatabricksUnityCatalogTableCreation:
    """Test Unity Catalog-specific table creation behavior."""

    @patch("app.databricks_adapter._build_workspace_client")
    def test_create_table_uses_delta_format(
        self, mock_build_client: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Table creation must specify USING DELTA for Unity Catalog compatibility."""
        monkeypatch.setenv("DATABRICKS_HOST", "https://adb-123.azuredatabricks.net")
        monkeypatch.setenv("DATABRICKS_TOKEN", "dapi-test")
        monkeypatch.setenv("DATABRICKS_CONFIG_PROFILE", "")
        monkeypatch.setenv("DATABRICKS_AUTH_TYPE", "")
        monkeypatch.setenv("DATABRICKS_CLIENT_ID", "")
        monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "")
        monkeypatch.setenv("DATABRICKS_CATALOG", "workspace")
        monkeypatch.setenv("DATABRICKS_SCHEMA", "lakehouse_lab")
        monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "wh-123")

        mock_client = MagicMock()
        mock_build_client.return_value = mock_client

        mock_response = MagicMock()
        mock_response.status.state.value = "SUCCEEDED"
        mock_client.statement_execution.execute_statement.return_value = mock_response

        export_gold_kpis_to_databricks(SAMPLE_GOLD_ROWS)

        # Find the CREATE TABLE call (third statement: after CREATE CATALOG and CREATE SCHEMA)
        calls = mock_client.statement_execution.execute_statement.call_args_list
        create_table_call = calls[2]
        sql = create_table_call.kwargs.get("statement", "")
        assert "USING DELTA" in sql
        assert "`workspace`.`lakehouse_lab`.`region_kpis`" in sql

    @patch("app.databricks_adapter._build_workspace_client")
    def test_merge_upsert_targets_correct_table(
        self, mock_build_client: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """MERGE statement must target the fully qualified Unity Catalog table name."""
        monkeypatch.setenv("DATABRICKS_HOST", "https://adb-123.azuredatabricks.net")
        monkeypatch.setenv("DATABRICKS_TOKEN", "dapi-test")
        monkeypatch.setenv("DATABRICKS_CONFIG_PROFILE", "")
        monkeypatch.setenv("DATABRICKS_AUTH_TYPE", "")
        monkeypatch.setenv("DATABRICKS_CLIENT_ID", "")
        monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "")
        monkeypatch.setenv("DATABRICKS_CATALOG", "workspace")
        monkeypatch.setenv("DATABRICKS_SCHEMA", "lakehouse_lab")
        monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "wh-123")

        mock_client = MagicMock()
        mock_build_client.return_value = mock_client

        mock_response = MagicMock()
        mock_response.status.state.value = "SUCCEEDED"
        mock_client.statement_execution.execute_statement.return_value = mock_response

        export_gold_kpis_to_databricks(SAMPLE_GOLD_ROWS)

        # Find the MERGE call (fourth statement)
        calls = mock_client.statement_execution.execute_statement.call_args_list
        merge_call = calls[3]
        sql = merge_call.kwargs.get("statement", "")
        assert "MERGE INTO `workspace`.`lakehouse_lab`.`region_kpis`" in sql


# ---------------------------------------------------------------------------
# Query function tests
# ---------------------------------------------------------------------------


class TestDatabricksQueryFunction:
    """Test the query_region_kpis read function."""

    def test_query_returns_empty_when_not_configured(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """query_region_kpis must return empty list when not configured."""
        monkeypatch.setenv("DATABRICKS_HOST", "")
        monkeypatch.setenv("DATABRICKS_TOKEN", "")
        monkeypatch.setenv("DATABRICKS_CONFIG_PROFILE", "")
        monkeypatch.setenv("DATABRICKS_AUTH_TYPE", "")
        monkeypatch.setenv("DATABRICKS_CLIENT_ID", "")
        monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "")
        result = query_region_kpis()
        assert result == []
