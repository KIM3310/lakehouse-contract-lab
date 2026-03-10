from __future__ import annotations

import importlib

import pytest


def test_build_script_invokes_snowflake_export(monkeypatch) -> None:
    pytest.importorskip("delta")
    module = importlib.import_module("scripts.build_lakehouse_artifacts")
    called = {}

    def fake_export(rows):  # type: ignore[no-untyped-def]
        called["rows"] = rows
        return True

    monkeypatch.setattr(module, "export_gold_kpis_to_snowflake", fake_export)
    payload = {"rows": [{"region": "apac"}]}
    result = module.export_gold_kpis_to_snowflake(payload["rows"])

    assert result is True
    assert called["rows"] == [{"region": "apac"}]
