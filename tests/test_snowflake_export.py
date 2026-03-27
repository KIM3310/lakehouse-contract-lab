"""Mocked tests for the Snowflake export adapter.

These tests exercise the Snowflake adapter's connection, schema creation,
table creation, and MERGE upsert logic without requiring a live Snowflake
instance. All Snowflake SDK calls are mocked.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from app.snowflake_adapter import (
    GOLD_TABLE_NAME,
    _create_gold_table,
    _ensure_schema,
    _get_connection_params,
    _upsert_rows,
    export_gold_kpis_to_snowflake,
    is_configured,
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


class TestSnowflakeConfiguration:
    """Test env-var gating and connection parameter building."""

    def test_is_configured_returns_false_when_env_missing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Adapter must report unconfigured when SNOWFLAKE_ACCOUNT is empty."""
        monkeypatch.setattr("app.snowflake_adapter.SNOWFLAKE_ACCOUNT", "")
        monkeypatch.setattr("app.snowflake_adapter.SNOWFLAKE_USER", "")
        monkeypatch.setattr("app.snowflake_adapter.SNOWFLAKE_PASSWORD", "")
        assert is_configured() is False

    def test_is_configured_returns_true_when_env_set(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Adapter must report configured when all required vars are present."""
        monkeypatch.setattr("app.snowflake_adapter.SNOWFLAKE_ACCOUNT", "test_account")
        monkeypatch.setattr("app.snowflake_adapter.SNOWFLAKE_USER", "test_user")
        monkeypatch.setattr("app.snowflake_adapter.SNOWFLAKE_PASSWORD", "test_pass")
        assert is_configured() is True

    def test_get_connection_params_returns_expected_keys(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Connection params dict must contain all required Snowflake keys."""
        monkeypatch.setattr("app.snowflake_adapter.SNOWFLAKE_ACCOUNT", "acct")
        monkeypatch.setattr("app.snowflake_adapter.SNOWFLAKE_USER", "usr")
        monkeypatch.setattr("app.snowflake_adapter.SNOWFLAKE_PASSWORD", "pw")
        monkeypatch.setattr("app.snowflake_adapter.SNOWFLAKE_WAREHOUSE", "WH")
        monkeypatch.setattr("app.snowflake_adapter.SNOWFLAKE_DATABASE", "DB")
        monkeypatch.setattr("app.snowflake_adapter.SNOWFLAKE_SCHEMA", "SCH")
        monkeypatch.setattr("app.snowflake_adapter.SNOWFLAKE_ROLE", "ROLE")
        params = _get_connection_params()
        assert params == {
            "account": "acct",
            "user": "usr",
            "password": "pw",
            "warehouse": "WH",
            "database": "DB",
            "schema": "SCH",
            "role": "ROLE",
        }


# ---------------------------------------------------------------------------
# Schema and table creation tests
# ---------------------------------------------------------------------------


class TestSnowflakeSchemaAndTable:
    """Test DDL execution for schema and gold table creation."""

    def test_ensure_schema_executes_ddl(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """_ensure_schema must execute CREATE DATABASE and CREATE SCHEMA."""
        monkeypatch.setattr("app.snowflake_adapter.SNOWFLAKE_DATABASE", "TEST_DB")
        monkeypatch.setattr("app.snowflake_adapter.SNOWFLAKE_SCHEMA", "TEST_SCHEMA")
        cursor = MagicMock()
        _ensure_schema(cursor)
        executed_stmts = [call.args[0] for call in cursor.execute.call_args_list]
        assert any("CREATE DATABASE IF NOT EXISTS TEST_DB" in stmt for stmt in executed_stmts)
        assert any("CREATE SCHEMA IF NOT EXISTS TEST_SCHEMA" in stmt for stmt in executed_stmts)
        assert any("USE DATABASE TEST_DB" in stmt for stmt in executed_stmts)
        assert any("USE SCHEMA TEST_SCHEMA" in stmt for stmt in executed_stmts)

    def test_create_gold_table_executes_ddl(self) -> None:
        """_create_gold_table must execute CREATE TABLE IF NOT EXISTS."""
        cursor = MagicMock()
        _create_gold_table(cursor)
        executed_sql = cursor.execute.call_args[0][0]
        assert f"CREATE TABLE IF NOT EXISTS {GOLD_TABLE_NAME}" in executed_sql
        assert "region" in executed_sql
        assert "gross_revenue_usd" in executed_sql
        assert "loaded_at" in executed_sql


# ---------------------------------------------------------------------------
# MERGE upsert tests
# ---------------------------------------------------------------------------


class TestSnowflakeMergeUpsert:
    """Test the MERGE-based upsert logic."""

    def test_upsert_rows_executes_merge(self) -> None:
        """_upsert_rows must execute a MERGE INTO statement with correct params."""
        cursor = MagicMock()
        cursor.rowcount = 2
        result = _upsert_rows(cursor, SAMPLE_GOLD_ROWS)
        assert result == 2
        merge_sql = cursor.execute.call_args[0][0]
        assert "MERGE INTO" in merge_sql
        assert "WHEN MATCHED THEN UPDATE" in merge_sql
        assert "WHEN NOT MATCHED THEN INSERT" in merge_sql
        # Verify params contain expected values
        params = cursor.execute.call_args[0][1]
        assert "KR-SEOUL" in params
        assert "US-WEST" in params

    def test_upsert_rows_returns_zero_for_empty_input(self) -> None:
        """_upsert_rows must return 0 and skip execution when given no rows."""
        cursor = MagicMock()
        result = _upsert_rows(cursor, [])
        assert result == 0
        cursor.execute.assert_not_called()

    def test_upsert_rows_falls_back_to_len_when_rowcount_none(self) -> None:
        """_upsert_rows must use len(rows) as fallback when cursor.rowcount is None."""
        cursor = MagicMock()
        cursor.rowcount = None
        result = _upsert_rows(cursor, SAMPLE_GOLD_ROWS)
        assert result == len(SAMPLE_GOLD_ROWS)

    def test_upsert_merge_matches_on_region(self) -> None:
        """The MERGE statement must use region as the join key."""
        cursor = MagicMock()
        cursor.rowcount = 1
        _upsert_rows(cursor, SAMPLE_GOLD_ROWS[:1])
        merge_sql = cursor.execute.call_args[0][0]
        assert "target.region = source.region" in merge_sql


# ---------------------------------------------------------------------------
# End-to-end export function tests
# ---------------------------------------------------------------------------


class TestSnowflakeExportFunction:
    """Test the public export_gold_kpis_to_snowflake function."""

    def test_export_skips_when_not_configured(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Export must return False and not connect when env vars are missing."""
        monkeypatch.setattr("app.snowflake_adapter.SNOWFLAKE_ACCOUNT", "")
        monkeypatch.setattr("app.snowflake_adapter.SNOWFLAKE_USER", "")
        monkeypatch.setattr("app.snowflake_adapter.SNOWFLAKE_PASSWORD", "")
        result = export_gold_kpis_to_snowflake(SAMPLE_GOLD_ROWS)
        assert result is False

    def test_export_returns_false_when_connector_missing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Export must return False when snowflake-connector-python is not installed."""
        monkeypatch.setattr("app.snowflake_adapter.SNOWFLAKE_ACCOUNT", "acct")
        monkeypatch.setattr("app.snowflake_adapter.SNOWFLAKE_USER", "usr")
        monkeypatch.setattr("app.snowflake_adapter.SNOWFLAKE_PASSWORD", "pw")

        import builtins

        real_import = builtins.__import__

        def mock_import(name: str, *args: Any, **kwargs: Any) -> Any:
            if name == "snowflake.connector":
                raise ImportError("mocked missing connector")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", mock_import)
        result = export_gold_kpis_to_snowflake(SAMPLE_GOLD_ROWS)
        assert result is False

    @patch("app.snowflake_adapter._upsert_rows")
    @patch("app.snowflake_adapter._create_gold_table")
    @patch("app.snowflake_adapter._ensure_schema")
    def test_export_success_with_mocked_connector(
        self,
        mock_ensure: MagicMock,
        mock_create: MagicMock,
        mock_upsert: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Export must call schema setup, table creation, and upsert when configured."""
        monkeypatch.setattr("app.snowflake_adapter.SNOWFLAKE_ACCOUNT", "acct")
        monkeypatch.setattr("app.snowflake_adapter.SNOWFLAKE_USER", "usr")
        monkeypatch.setattr("app.snowflake_adapter.SNOWFLAKE_PASSWORD", "pw")

        mock_connector = MagicMock()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connector.connect.return_value = mock_conn

        mock_snowflake = MagicMock()
        mock_snowflake.connector = mock_connector

        with patch.dict("sys.modules", {"snowflake": mock_snowflake, "snowflake.connector": mock_connector}):
            result = export_gold_kpis_to_snowflake(SAMPLE_GOLD_ROWS)

        assert result is True
        mock_ensure.assert_called_once_with(mock_cursor)
        mock_create.assert_called_once_with(mock_cursor)
        mock_upsert.assert_called_once_with(mock_cursor, SAMPLE_GOLD_ROWS)
        mock_conn.close.assert_called_once()

    @patch("app.snowflake_adapter._ensure_schema", side_effect=RuntimeError("connection failed"))
    def test_export_returns_false_on_exception(
        self,
        mock_ensure: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Export must return False and not raise when an exception occurs."""
        monkeypatch.setattr("app.snowflake_adapter.SNOWFLAKE_ACCOUNT", "acct")
        monkeypatch.setattr("app.snowflake_adapter.SNOWFLAKE_USER", "usr")
        monkeypatch.setattr("app.snowflake_adapter.SNOWFLAKE_PASSWORD", "pw")

        mock_connector = MagicMock()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connector.connect.return_value = mock_conn

        mock_snowflake = MagicMock()
        mock_snowflake.connector = mock_connector

        with patch.dict("sys.modules", {"snowflake": mock_snowflake, "snowflake.connector": mock_connector}):
            result = export_gold_kpis_to_snowflake(SAMPLE_GOLD_ROWS)

        assert result is False
