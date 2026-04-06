"""Resource pack loader tests."""

from __future__ import annotations

from app.resource_pack import (
    data_files,
    external_data_summary,
    load_export_targets,
    load_quality_rules,
    load_source_rows,
    load_validation_cases,
    resource_pack_summary,
)


def test_source_rows_loaded_from_csv() -> None:
    rows = load_source_rows()
    assert len(rows) == 12
    assert any(row["customer_id"] is None for row in rows)


def test_quality_rules_and_targets_are_nonempty() -> None:
    assert len(load_quality_rules()) >= 4
    assert len(load_export_targets()) >= 4
    assert len(load_validation_cases()) >= 4


def test_resource_pack_summary_counts_match() -> None:
    summary = resource_pack_summary()
    assert summary["source_row_count"] == 12
    assert summary["quality_rule_count"] >= 4
    assert summary["export_target_count"] >= 4


def test_all_resource_files_exist() -> None:
    for path in data_files().values():
        assert path.exists()


def test_external_data_summary_handles_staged_files() -> None:
    summary = external_data_summary()
    assert "files" in summary
    assert "olist_orders_dataset.csv" in summary["files"]
    assert "preview_rows" in summary["files"]["olist_orders_dataset.csv"]
