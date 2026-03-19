"""Databricks connector for exporting gold-layer KPIs to Unity Catalog.

This module is **env-var gated**: it only activates when ``DATABRICKS_HOST``
is present in the environment.  When the variable is absent, all public
functions return gracefully without side effects.

Typical usage from the pipeline entry-point::

    from app.databricks_adapter import export_gold_kpis_to_databricks
    export_gold_kpis_to_databricks(gold_rows)

Required environment variables (all must be set for the connector to activate):
    DATABRICKS_HOST, DATABRICKS_TOKEN

Optional:
    DATABRICKS_CLUSTER_ID  (used for command execution context)
    DATABRICKS_CATALOG     (default ``main``)
    DATABRICKS_SCHEMA      (default ``lakehouse_lab``)
"""

from __future__ import annotations

import logging
import os
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DATABRICKS_HOST: str = os.getenv("DATABRICKS_HOST", "")
DATABRICKS_TOKEN: str = os.getenv("DATABRICKS_TOKEN", "")
DATABRICKS_CLUSTER_ID: str = os.getenv("DATABRICKS_CLUSTER_ID", "")
DATABRICKS_CATALOG: str = os.getenv("DATABRICKS_CATALOG", "main")
DATABRICKS_SCHEMA: str = os.getenv("DATABRICKS_SCHEMA", "lakehouse_lab")

GOLD_TABLE_NAME: str = "region_kpis"


def is_configured() -> bool:
    """Return True when all required Databricks env vars are present."""
    return all([DATABRICKS_HOST, DATABRICKS_TOKEN])


def _get_full_table_name() -> str:
    """Return the three-level Unity Catalog table name."""
    return f"{DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}.{GOLD_TABLE_NAME}"


def _ensure_schema(client: Any) -> None:
    """Create the target catalog and schema in Unity Catalog if needed.

    Args:
        client: An authenticated ``WorkspaceClient`` from the Databricks SDK.
    """
    from databricks.sdk.service.catalog import (  # type: ignore[import-untyped]
        CatalogInfo,
        SchemaInfo,
    )

    # Ensure catalog exists
    try:
        client.catalogs.get(DATABRICKS_CATALOG)
        logger.debug("Catalog %s already exists", DATABRICKS_CATALOG)
    except Exception:
        logger.info("Creating catalog %s", DATABRICKS_CATALOG)
        client.catalogs.create(
            CatalogInfo(name=DATABRICKS_CATALOG, comment="Lakehouse Contract Lab")
        )

    # Ensure schema exists
    full_schema = f"{DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}"
    try:
        client.schemas.get(full_schema)
        logger.debug("Schema %s already exists", full_schema)
    except Exception:
        logger.info("Creating schema %s", full_schema)
        client.schemas.create(
            SchemaInfo(
                name=DATABRICKS_SCHEMA,
                catalog_name=DATABRICKS_CATALOG,
                comment="Gold-layer KPI tables from Lakehouse Contract Lab",
            )
        )


def _build_create_table_sql() -> str:
    """Return the SQL DDL for the gold KPI table."""
    table = _get_full_table_name()
    return f"""
    CREATE TABLE IF NOT EXISTS {table} (
        region              STRING        NOT NULL,
        gross_revenue_usd   DOUBLE        NOT NULL,
        accepted_orders     BIGINT        NOT NULL,
        completed_orders    BIGINT        NOT NULL,
        pipeline_orders     BIGINT        NOT NULL,
        distinct_customers  BIGINT        NOT NULL,
        loaded_at           TIMESTAMP     DEFAULT current_timestamp()
    )
    USING DELTA
    COMMENT 'Region-level KPI aggregations from the Lakehouse Contract Lab pipeline'
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true'
    )
    """


def _build_merge_sql(rows: list[dict[str, Any]]) -> str:
    """Build a MERGE INTO statement for upserting gold KPI rows.

    Args:
        rows: Gold KPI rows as a list of dicts.

    Returns:
        A SQL MERGE statement string.
    """
    table = _get_full_table_name()

    value_rows: list[str] = []
    for row in rows:
        value_rows.append(
            f"  ('{row['region']}', {row['gross_revenue_usd']}, "
            f"{row['accepted_orders']}, {row['completed_orders']}, "
            f"{row['pipeline_orders']}, {row['distinct_customers']})"
        )

    values_block = ",\n".join(value_rows)

    return f"""
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
        completed_orders, pipeline_orders, distinct_customers
    ) VALUES (
        source.region, source.gross_revenue_usd, source.accepted_orders,
        source.completed_orders, source.pipeline_orders, source.distinct_customers
    )
    """


def _execute_sql(client: Any, sql: str) -> Any:
    """Execute a SQL statement via the Databricks Statement Execution API.

    Args:
        client: An authenticated ``WorkspaceClient``.
        sql: The SQL statement to execute.

    Returns:
        The statement execution response.
    """
    warehouse_id = _get_sql_warehouse_id(client)
    if not warehouse_id:
        logger.error("No SQL warehouse found; cannot execute statement")
        raise RuntimeError("No Databricks SQL warehouse available")

    response = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=sql,
        wait_timeout="30s",
    )
    logger.debug("SQL execution status: %s", response.status)
    return response


def _get_sql_warehouse_id(client: Any) -> str | None:
    """Find the first available SQL warehouse ID.

    Args:
        client: An authenticated ``WorkspaceClient``.

    Returns:
        The warehouse ID string, or None if no warehouse is available.
    """
    try:
        warehouses = list(client.warehouses.list())
        if warehouses:
            warehouse_id = warehouses[0].id
            logger.debug("Using SQL warehouse: %s", warehouse_id)
            return warehouse_id
    except Exception:
        logger.warning("Failed to list SQL warehouses", exc_info=True)
    return None


def export_gold_kpis_to_databricks(rows: list[dict[str, Any]]) -> bool:
    """Export gold-layer KPI rows to Databricks Unity Catalog.

    This is the main public entry point.  When Databricks is not configured
    (``DATABRICKS_HOST`` is absent), the function logs a skip message and
    returns ``False`` without raising.

    Args:
        rows: Gold KPI rows as a list of dicts. Each dict should contain:
              ``region``, ``gross_revenue_usd``, ``accepted_orders``,
              ``completed_orders``, ``pipeline_orders``, ``distinct_customers``.

    Returns:
        ``True`` if rows were successfully written, ``False`` otherwise.
    """
    if not is_configured():
        logger.info(
            "Databricks export skipped: DATABRICKS_HOST not set. "
            "Set DATABRICKS_HOST and DATABRICKS_TOKEN to enable."
        )
        return False

    if not rows:
        logger.warning("No gold KPI rows to export")
        return False

    try:
        from databricks.sdk import WorkspaceClient  # type: ignore[import-untyped]
    except ImportError:
        logger.warning(
            "databricks-sdk is not installed. "
            "Install it with: pip install databricks-sdk"
        )
        return False

    try:
        logger.info(
            "Connecting to Databricks host=%s catalog=%s.%s",
            DATABRICKS_HOST,
            DATABRICKS_CATALOG,
            DATABRICKS_SCHEMA,
        )
        client = WorkspaceClient(
            host=DATABRICKS_HOST,
            token=DATABRICKS_TOKEN,
        )

        _ensure_schema(client)

        # Create table
        create_sql = _build_create_table_sql()
        _execute_sql(client, create_sql)
        logger.info("Ensured gold table %s exists", _get_full_table_name())

        # Merge rows
        merge_sql = _build_merge_sql(rows)
        _execute_sql(client, merge_sql)
        logger.info(
            "Upserted %d gold KPI rows into Databricks Unity Catalog", len(rows)
        )

        return True

    except Exception:
        logger.exception("Databricks export failed")
        return False
