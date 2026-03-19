"""Snowflake connector for exporting gold-layer KPIs.

This module is **env-var gated**: it only activates when ``SNOWFLAKE_ACCOUNT``
is present in the environment.  When the variable is absent, all public
functions return gracefully without side effects, so the rest of the pipeline
is never blocked by a missing Snowflake configuration.

Typical usage from the pipeline entry-point::

    from app.snowflake_adapter import export_gold_kpis_to_snowflake
    export_gold_kpis_to_snowflake(gold_rows)

Required environment variables (all must be set for the connector to activate):
    SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD,
    SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE

Optional:
    SNOWFLAKE_SCHEMA  (default ``GOLD``)
    SNOWFLAKE_ROLE    (default ``SYSADMIN``)
"""

from __future__ import annotations

import logging
import os
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SNOWFLAKE_ACCOUNT: str = os.getenv("SNOWFLAKE_ACCOUNT", "")
SNOWFLAKE_USER: str = os.getenv("SNOWFLAKE_USER", "")
SNOWFLAKE_PASSWORD: str = os.getenv("SNOWFLAKE_PASSWORD", "")
SNOWFLAKE_WAREHOUSE: str = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
SNOWFLAKE_DATABASE: str = os.getenv("SNOWFLAKE_DATABASE", "LAKEHOUSE_LAB")
SNOWFLAKE_SCHEMA: str = os.getenv("SNOWFLAKE_SCHEMA", "GOLD")
SNOWFLAKE_ROLE: str = os.getenv("SNOWFLAKE_ROLE", "SYSADMIN")

GOLD_TABLE_NAME: str = "REGION_KPIS"


def is_configured() -> bool:
    """Return True when all required Snowflake env vars are present."""
    return all([SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD])


def _get_connection_params() -> dict[str, str]:
    """Build the Snowflake connection parameter dictionary."""
    return {
        "account": SNOWFLAKE_ACCOUNT,
        "user": SNOWFLAKE_USER,
        "password": SNOWFLAKE_PASSWORD,
        "warehouse": SNOWFLAKE_WAREHOUSE,
        "database": SNOWFLAKE_DATABASE,
        "schema": SNOWFLAKE_SCHEMA,
        "role": SNOWFLAKE_ROLE,
    }


def _ensure_schema(cursor: Any) -> None:
    """Create the target database and schema if they do not exist.

    Args:
        cursor: An active Snowflake cursor object.
    """
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {SNOWFLAKE_DATABASE}")
    cursor.execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {SNOWFLAKE_SCHEMA}")
    cursor.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")
    logger.info(
        "Ensured Snowflake schema %s.%s exists",
        SNOWFLAKE_DATABASE,
        SNOWFLAKE_SCHEMA,
    )


def _create_gold_table(cursor: Any) -> None:
    """Create the gold KPI table if it does not exist.

    Args:
        cursor: An active Snowflake cursor object.
    """
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {GOLD_TABLE_NAME} (
        region              VARCHAR(64)   NOT NULL,
        gross_revenue_usd   NUMBER(18,2)  NOT NULL,
        accepted_orders     INTEGER       NOT NULL,
        completed_orders    INTEGER       NOT NULL,
        pipeline_orders     INTEGER       NOT NULL,
        distinct_customers  INTEGER       NOT NULL,
        loaded_at           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
    """
    cursor.execute(ddl)
    logger.info("Ensured gold table %s exists", GOLD_TABLE_NAME)


def _upsert_rows(cursor: Any, rows: list[dict[str, Any]]) -> int:
    """Merge gold KPI rows into the Snowflake table using MERGE.

    Args:
        cursor: An active Snowflake cursor object.
        rows: List of gold KPI row dicts with keys matching the table columns.

    Returns:
        Number of rows upserted.
    """
    if not rows:
        logger.warning("No gold KPI rows to export")
        return 0

    # Stage rows via INSERT ... SELECT with VALUES
    values_clauses: list[str] = []
    for row in rows:
        values_clauses.append(
            f"('{row['region']}', {row['gross_revenue_usd']}, "
            f"{row['accepted_orders']}, {row['completed_orders']}, "
            f"{row['pipeline_orders']}, {row['distinct_customers']})"
        )

    merge_sql = f"""
    MERGE INTO {GOLD_TABLE_NAME} AS target
    USING (
        SELECT
            column1  AS region,
            column2  AS gross_revenue_usd,
            column3  AS accepted_orders,
            column4  AS completed_orders,
            column5  AS pipeline_orders,
            column6  AS distinct_customers
        FROM VALUES {', '.join(values_clauses)}
    ) AS source
    ON target.region = source.region
    WHEN MATCHED THEN UPDATE SET
        gross_revenue_usd  = source.gross_revenue_usd,
        accepted_orders    = source.accepted_orders,
        completed_orders   = source.completed_orders,
        pipeline_orders    = source.pipeline_orders,
        distinct_customers = source.distinct_customers,
        loaded_at          = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        region, gross_revenue_usd, accepted_orders,
        completed_orders, pipeline_orders, distinct_customers
    ) VALUES (
        source.region, source.gross_revenue_usd, source.accepted_orders,
        source.completed_orders, source.pipeline_orders, source.distinct_customers
    )
    """
    cursor.execute(merge_sql)
    affected: int = cursor.rowcount or len(rows)
    logger.info("Upserted %d gold KPI rows into Snowflake", affected)
    return affected


def export_gold_kpis_to_snowflake(rows: list[dict[str, Any]]) -> bool:
    """Export gold-layer KPI rows to Snowflake.

    This is the main public entry point. When Snowflake is not configured
    (``SNOWFLAKE_ACCOUNT`` is absent), the function logs a skip message and
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
            "Snowflake export skipped: SNOWFLAKE_ACCOUNT not set. "
            "Set SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, and SNOWFLAKE_PASSWORD to enable."
        )
        return False

    try:
        import snowflake.connector  # type: ignore[import-untyped]
    except ImportError:
        logger.warning(
            "snowflake-connector-python is not installed. "
            "Install it with: pip install snowflake-connector-python"
        )
        return False

    conn = None
    try:
        logger.info(
            "Connecting to Snowflake account=%s warehouse=%s",
            SNOWFLAKE_ACCOUNT,
            SNOWFLAKE_WAREHOUSE,
        )
        conn = snowflake.connector.connect(**_get_connection_params())
        cursor = conn.cursor()

        _ensure_schema(cursor)
        _create_gold_table(cursor)
        _upsert_rows(cursor, rows)

        cursor.close()
        logger.info("Snowflake export completed successfully")
        return True

    except Exception:
        logger.exception("Snowflake export failed")
        return False

    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                logger.warning("Failed to close Snowflake connection", exc_info=True)
