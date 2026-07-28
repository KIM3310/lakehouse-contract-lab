"""Databricks Unity Catalog export adapter. Env-var gated -- no-op when DATABRICKS_HOST is unset."""

from __future__ import annotations

import logging
import os
from typing import Any

logger = logging.getLogger(__name__)

GOLD_TABLE_NAME: str = "region_kpis"


def _settings() -> dict[str, str]:
    return {
        "host": os.getenv("DATABRICKS_HOST", "").strip().rstrip("/"),
        "token": os.getenv("DATABRICKS_TOKEN", "").strip(),
        "client_id": os.getenv("DATABRICKS_CLIENT_ID", "").strip(),
        "client_secret": os.getenv("DATABRICKS_CLIENT_SECRET", "").strip(),
        "auth_type": os.getenv("DATABRICKS_AUTH_TYPE", "").strip(),
        "profile": os.getenv("DATABRICKS_CONFIG_PROFILE", "").strip(),
        "warehouse_id": os.getenv("DATABRICKS_WAREHOUSE_ID", "").strip(),
        "catalog": os.getenv("DATABRICKS_CATALOG", "main").strip(),
        "schema": os.getenv("DATABRICKS_SCHEMA", "lakehouse_lab").strip(),
    }


def is_configured() -> bool:
    settings = _settings()
    return bool(
        settings["host"]
        and (
            settings["token"]
            or settings["profile"]
            or settings["auth_type"]
            or (settings["client_id"] and settings["client_secret"])
        )
    )


def _quote(name: str) -> str:
    safe = name.replace("`", "``")
    return f"`{safe}`"


def _get_full_table_name() -> str:
    settings = _settings()
    return ".".join(_quote(part) for part in (settings["catalog"], settings["schema"], GOLD_TABLE_NAME))


def _build_workspace_client() -> Any:
    try:
        from databricks.sdk import WorkspaceClient  # type: ignore[import-untyped]
    except ImportError as exc:  # pragma: no cover - env-specific
        raise RuntimeError("databricks-sdk is not installed") from exc

    settings = _settings()
    if settings["token"]:
        return WorkspaceClient(host=settings["host"], token=settings["token"])
    if settings["client_id"] and settings["client_secret"]:
        return WorkspaceClient(
            host=settings["host"],
            client_id=settings["client_id"],
            client_secret=settings["client_secret"],
        )
    if settings["profile"]:
        return WorkspaceClient(profile=settings["profile"])
    return WorkspaceClient(host=settings["host"])


def _state_value(response: Any) -> str:
    state = getattr(getattr(response, "status", None), "state", None)
    return getattr(state, "value", str(state or "")).upper()


def _statement_error_message(response: Any) -> str:
    error = getattr(getattr(response, "status", None), "error", None)
    if not error:
        return "Unknown Databricks statement failure"
    message = getattr(error, "message", "")
    return message or str(error)


def _resolve_warehouse_id(client: Any) -> str:
    settings = _settings()
    if settings["warehouse_id"]:
        return settings["warehouse_id"]

    warehouses = list(client.warehouses.list())
    if not warehouses:
        raise RuntimeError("No Databricks SQL warehouse available")

    for warehouse in warehouses:
        if getattr(warehouse, "state", "") == "RUNNING" and getattr(warehouse, "id", None):
            return str(warehouse.id)
    warehouse_id = getattr(warehouses[0], "id", None)
    if not warehouse_id:
        raise RuntimeError("Databricks SQL warehouse ID unavailable")
    return str(warehouse_id)


def _execute_sql(client: Any, sql: str, parameters: list[dict[str, str]] | None = None) -> Any:
    warehouse_id = _resolve_warehouse_id(client)
    settings = _settings()
    statement_args: dict[str, Any] = {
        "warehouse_id": warehouse_id,
        "statement": sql,
        "catalog": settings["catalog"],
        "schema": settings["schema"],
        "wait_timeout": "30s",
    }
    if parameters is not None:
        statement_args["parameters"] = parameters

    response = client.statement_execution.execute_statement(**statement_args)
    if _state_value(response) != "SUCCEEDED":
        raise RuntimeError(_statement_error_message(response))
    return response


def _ensure_schema(client: Any) -> None:
    settings = _settings()
    catalog_fqn = _quote(settings["catalog"])
    schema_fqn = f"{catalog_fqn}.{_quote(settings['schema'])}"
    _execute_sql(client, f"CREATE CATALOG IF NOT EXISTS {catalog_fqn}")
    _execute_sql(client, f"CREATE SCHEMA IF NOT EXISTS {schema_fqn}")


def _build_create_table_sql() -> str:
    table = _get_full_table_name()
    return f"""
    CREATE TABLE IF NOT EXISTS {table} (
        region              STRING        NOT NULL,
        gross_revenue_usd   DOUBLE        NOT NULL,
        accepted_orders     BIGINT        NOT NULL,
        completed_orders    BIGINT        NOT NULL,
        pipeline_orders     BIGINT        NOT NULL,
        distinct_customers  BIGINT        NOT NULL,
        loaded_at           TIMESTAMP
    )
    USING DELTA
    COMMENT 'Region-level KPI aggregations from the Lakehouse Contract Lab pipeline'
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true'
    )
    """


def _build_merge_statement(rows: list[dict[str, Any]]) -> tuple[str, list[dict[str, str]]]:
    table = _get_full_table_name()
    value_rows: list[str] = []
    parameters: list[dict[str, str]] = []
    parameter_specs = (
        ("region", "STRING", str),
        ("gross_revenue_usd", "DOUBLE", float),
        ("accepted_orders", "BIGINT", int),
        ("completed_orders", "BIGINT", int),
        ("pipeline_orders", "BIGINT", int),
        ("distinct_customers", "BIGINT", int),
    )
    for index, row in enumerate(rows):
        markers: list[str] = []
        for column_name, parameter_type, caster in parameter_specs:
            parameter_name = f"{column_name}_{index}"
            markers.append(f":{parameter_name}")
            parameters.append(
                {
                    "name": parameter_name,
                    "value": str(caster(row[column_name])),
                    "type": parameter_type,
                }
            )
        value_rows.append(
            f"  ({', '.join(markers)})"
        )

    values_block = ",\n".join(value_rows)
    sql = f"""
    MERGE INTO {table} AS target
    USING (
        SELECT * FROM (
            VALUES
{values_block}
        ) AS src(region, gross_revenue_usd, accepted_orders,
                 completed_orders, pipeline_orders, distinct_customers)
    ) AS source
    ON target.region = source.region
    WHEN MATCHED THEN UPDATE SET
        target.gross_revenue_usd  = source.gross_revenue_usd,
        target.accepted_orders    = source.accepted_orders,
        target.completed_orders   = source.completed_orders,
        target.pipeline_orders    = source.pipeline_orders,
        target.distinct_customers = source.distinct_customers,
        target.loaded_at          = current_timestamp()
    WHEN NOT MATCHED THEN INSERT (
        region, gross_revenue_usd, accepted_orders,
        completed_orders, pipeline_orders, distinct_customers, loaded_at
    ) VALUES (
        source.region, source.gross_revenue_usd, source.accepted_orders,
        source.completed_orders, source.pipeline_orders, source.distinct_customers, current_timestamp()
    )
    """  # nosec B608
    return sql, parameters


def _build_merge_sql(rows: list[dict[str, Any]]) -> str:
    sql, _ = _build_merge_statement(rows)
    return sql


def query_region_kpis(limit: int = 20) -> list[dict[str, Any]]:
    if not is_configured():
        return []
    client = _build_workspace_client()
    safe_limit = max(1, min(int(limit), 1000))
    response = _execute_sql(
        client,
        f"SELECT region, gross_revenue_usd, accepted_orders, completed_orders, pipeline_orders, distinct_customers, loaded_at FROM {_get_full_table_name()} ORDER BY region LIMIT {safe_limit}",  # nosec B608
    )
    schema = getattr(getattr(response, "manifest", None), "schema", None)
    columns = [column.name.lower() for column in getattr(schema, "columns", [])]
    rows = getattr(getattr(response, "result", None), "data_array", None) or []
    return [dict(zip(columns, row, strict=True)) for row in rows]


def export_gold_kpis_to_databricks(rows: list[dict[str, Any]]) -> bool:
    if not is_configured():
        logger.info(
            "Databricks export skipped: set DATABRICKS_HOST with a CLI profile, service-principal OAuth pair, or token."
        )
        return False
    if not rows:
        logger.warning("No gold KPI rows to export")
        return False

    try:
        settings = _settings()
        logger.info(
            "Connecting to Databricks host=%s catalog=%s.%s",
            settings["host"],
            settings["catalog"],
            settings["schema"],
        )
        client = _build_workspace_client()
        _ensure_schema(client)
        _execute_sql(client, _build_create_table_sql())
        merge_sql, merge_parameters = _build_merge_statement(rows)
        _execute_sql(client, merge_sql, merge_parameters)
        logger.info("Upserted %d gold KPI rows into Databricks Unity Catalog", len(rows))
        return True
    except Exception:
        logger.exception("Databricks export failed")
        return False
