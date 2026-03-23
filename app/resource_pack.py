"""Pure-Python loaders for the built-in synthetic resource pack."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"


def data_files() -> dict[str, Path]:
    return {
        "source_orders": DATA_DIR / "source_orders.csv",
        "quality_rules": DATA_DIR / "quality_rules.json",
        "export_targets": DATA_DIR / "export_targets.json",
        "validation_cases": DATA_DIR / "validation_cases.json",
    }


def load_source_rows() -> list[dict[str, Any]]:
    with data_files()["source_orders"].open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    parsed: list[dict[str, Any]] = []
    for row in rows:
        parsed.append(
            {
                "order_id": row["order_id"],
                "customer_id": row["customer_id"] or None,
                "region": row["region"] or None,
                "channel": row["channel"],
                "status": row["status"],
                "amount": float(row["amount"]),
                "currency": row["currency"],
                "order_ts": row["order_ts"],
            }
        )
    return parsed


def load_quality_rules() -> list[dict[str, Any]]:
    return json.loads(data_files()["quality_rules"].read_text(encoding="utf-8"))


def load_export_targets() -> list[dict[str, Any]]:
    return json.loads(data_files()["export_targets"].read_text(encoding="utf-8"))


def load_validation_cases() -> list[dict[str, Any]]:
    return json.loads(data_files()["validation_cases"].read_text(encoding="utf-8"))


def resource_pack_summary() -> dict[str, Any]:
    return {
        "source_row_count": len(load_source_rows()),
        "quality_rule_count": len(load_quality_rules()),
        "export_target_count": len(load_export_targets()),
        "validation_case_count": len(load_validation_cases()),
    }
