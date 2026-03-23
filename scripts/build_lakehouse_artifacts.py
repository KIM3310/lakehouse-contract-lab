"""Build lakehouse medallion pipeline artifacts using Spark and Delta Lake.

Executes the full bronze -> silver -> gold medallion pipeline on sample order
data, applies quality gates, writes Delta tables, and generates JSON artifacts
plus an SVG architecture board for reviewer consumption.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import urllib.error
import urllib.request
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

from delta import configure_spark_with_delta_pip
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F  # noqa: N812

from app.resource_pack import (
    data_files,
    load_export_targets,
    load_quality_rules,
    load_source_rows,
    load_validation_cases,
    resource_pack_summary,
)

ROOT: Path = Path(__file__).resolve().parents[1]
ARTIFACTS_DIR: Path = ROOT / "artifacts"
DELTA_DIR: Path = ARTIFACTS_DIR / "runtime_delta"
DOCS_DIR: Path = ROOT / "docs"

NOW: datetime = datetime.now(timezone.utc).replace(microsecond=0)
OPENAI_BASE_URL: str = "https://api.openai.com/v1"
DEFAULT_OPENAI_REFRESH_MODEL: str = "gpt-4o"

SOURCE_ROWS: list[dict[str, Any]] = load_source_rows()


def ensure_java_home() -> None:
    """Detect and set JAVA_HOME if not already configured.

    Also sets ``SPARK_LOCAL_IP`` to ``127.0.0.1`` for local Spark execution.
    """
    if os.environ.get("JAVA_HOME"):
        logger.debug("JAVA_HOME already set: %s", os.environ["JAVA_HOME"])
        os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
        return
    java_home = Path("/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home")
    if java_home.exists():
        os.environ["JAVA_HOME"] = str(java_home)
        logger.info("Auto-detected JAVA_HOME: %s", java_home)
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")


def java_runtime_available() -> bool:
    """Return True when a usable local Java runtime is available."""
    ensure_java_home()
    java_cmd = shutil.which("java")
    if not java_cmd:
        return False
    try:
        result = subprocess.run(
            [java_cmd, "-version"],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
    except OSError:
        return False
    return result.returncode == 0


def validate_prebuilt_artifacts() -> None:
    """Validate the checked-in artifact set for no-Java local environments."""
    required_paths = [
        ARTIFACTS_DIR / "lakehouse-proof-pack.json",
        ARTIFACTS_DIR / "quality-report.json",
        ARTIFACTS_DIR / "review-summary.json",
        ARTIFACTS_DIR / "bronze-preview.json",
        ARTIFACTS_DIR / "silver-preview.json",
        ARTIFACTS_DIR / "gold-preview.json",
        DOCS_DIR / "lakehouse-contract-board.svg",
    ]
    missing = [str(path.relative_to(ROOT)) for path in required_paths if not path.exists()]
    if missing:
        raise RuntimeError(
            "Java runtime unavailable and required prebuilt artifacts are missing: "
            + ", ".join(missing)
        )
    logger.info(
        "Java runtime unavailable; validated existing prebuilt artifacts instead of rebuilding Spark/Delta outputs"
    )


def build_spark() -> SparkSession:
    """Create and return a local Spark session configured for Delta Lake.

    Returns:
        A ``SparkSession`` with Delta Lake extensions enabled.
    """
    ensure_java_home()
    logger.info("Building Spark session with Delta Lake support")
    builder = (
        SparkSession.builder.appName("lakehouse-contract-lab")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "2")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def normalize_value(value: Any) -> Any:
    """Normalize a value for JSON serialization.

    Converts ``datetime`` instances to ISO-8601 strings and ``Decimal``
    instances to plain ``float`` values.  All other types pass through.

    Args:
        value: The value to normalize.

    Returns:
        A JSON-serializable representation of *value*.
    """
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return value


def rows_to_json(
    df: DataFrame,
    order_by: list[str],
    limit: int = 5,
) -> list[dict[str, Any]]:
    """Collect Spark DataFrame rows as a list of JSON-serializable dicts.

    Args:
        df: Source DataFrame.
        order_by: Column names to sort by before collecting.
        limit: Maximum number of rows to return.

    Returns:
        A list of dictionaries with normalized values.
    """
    rows = (
        df.orderBy(*order_by).limit(limit).collect()
        if order_by
        else df.limit(limit).collect()
    )
    return [
        {key: normalize_value(value) for key, value in row.asDict().items()}
        for row in rows
    ]


def latest_delta_version(table_path: Path) -> int | None:
    """Return the latest Delta log version number for a table, or ``None``.

    Args:
        table_path: Root path of the Delta table.

    Returns:
        The highest commit version found in ``_delta_log/``, or ``None`` if
        no log entries exist.
    """
    log_dir: Path = table_path / "_delta_log"
    versions: list[int] = [
        int(path.stem)
        for path in log_dir.glob("*.json")
        if path.stem.isdigit()
    ]
    return max(versions) if versions else None


def write_json(path: Path, payload: dict[str, Any]) -> None:
    """Write a dictionary to a JSON file, creating parent directories as needed.

    Args:
        path: Destination file path.
        payload: Data to serialize.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    logger.info("Wrote artifact: %s", path.name)


def build_review_summary_artifact(
    proof_pack: dict[str, Any],
    quality_report: dict[str, Any],
) -> dict[str, Any]:
    """Build the review-summary artifact, optionally using OpenAI for enrichment.

    When an ``OPENAI_API_KEY`` environment variable is present, the summary
    is generated via the OpenAI chat completions API.  Otherwise a static
    fallback is returned.

    Args:
        proof_pack: The lakehouse proof-pack dictionary.
        quality_report: The quality report dictionary.

    Returns:
        A review-summary artifact dictionary conforming to
        ``lakehouse-review-summary-v1``.
    """
    fallback: dict[str, Any] = {
        "schema": "lakehouse-review-summary-v1",
        "service": proof_pack["service"],
        "generatedAt": NOW.isoformat(),
        "generationMode": "static-fallback",
        "headline": "Refresh-only reviewer summary for Spark + Delta medallion proof.",
        "summary": {
            "platformFit": "snowflake-and-databricks-reviewable",
            "qualityPosture": "quality-gates-visible",
            "handoffPosture": "reviewer-safe-read-only",
            "nextAction": "Check quality report and gold preview together before making platform claims.",
        },
        "reviewPath": [
            "/api/runtime/lakehouse-proof-pack",
            "/api/runtime/quality-report",
            "/api/runtime/table-preview/gold",
        ],
        "proofAssets": [
            "artifacts/lakehouse-proof-pack.json",
            "artifacts/quality-report.json",
            "docs/lakehouse-contract-board.svg",
        ],
    }
    api_key: str = str(os.getenv("OPENAI_API_KEY", "")).strip()
    if not api_key:
        logger.info("No OPENAI_API_KEY set; using static fallback for review summary")
        return fallback

    logger.info("Requesting OpenAI-enriched review summary")
    schema: dict[str, Any] = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "headline": {"type": "string"},
            "platformFit": {"type": "string"},
            "qualityPosture": {"type": "string"},
            "handoffPosture": {"type": "string"},
            "nextAction": {"type": "string"},
        },
        "required": [
            "headline",
            "platformFit",
            "qualityPosture",
            "handoffPosture",
            "nextAction",
        ],
    }
    request = urllib.request.Request(
        f"{OPENAI_BASE_URL}/chat/completions",
        data=json.dumps(
            {
                "model": str(os.getenv("OPENAI_MODEL_REFRESH", "")).strip() or DEFAULT_OPENAI_REFRESH_MODEL,
                "temperature": 0.2,
                "response_format": {
                    "type": "json_schema",
                    "json_schema": {
                        "name": "lakehouse_review_summary",
                        "schema": schema,
                    },
                },
                "messages": [
                    {
                        "role": "system",
                        "content": "Summarize the lakehouse pipeline output for a field engineering handoff. Return JSON only.",
                    },
                    {
                        "role": "user",
                        "content": json.dumps(
                            {
                                "proof_pack": proof_pack["summary"],
                                "quality_report": quality_report["summary"],
                            }
                        ),
                    },
                ],
            }
        ).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            body: dict[str, Any] = json.loads(response.read().decode("utf-8"))
        content: str = str(((body.get("choices") or [{}])[0].get("message") or {}).get("content") or "").strip()
        parsed: dict[str, str] = json.loads(content)
        logger.info("OpenAI review summary generated successfully")
        return {
            "schema": "lakehouse-review-summary-v1",
            "service": proof_pack["service"],
            "generatedAt": NOW.isoformat(),
            "generationMode": "openai-refresh",
            "headline": parsed["headline"],
            "summary": {
                "platformFit": parsed["platformFit"],
                "qualityPosture": parsed["qualityPosture"],
                "handoffPosture": parsed["handoffPosture"],
                "nextAction": parsed["nextAction"],
            },
            "reviewPath": fallback["reviewPath"],
            "proofAssets": fallback["proofAssets"],
        }
    except (urllib.error.HTTPError, urllib.error.URLError, json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
        logger.warning("OpenAI review-summary refresh failed (%s: %s); using static fallback", type(exc).__name__, exc)
        return fallback


def build_svg(proof_pack: dict[str, Any]) -> None:
    """Generate an SVG architecture board from the proof-pack summary.

    Writes ``docs/lakehouse-contract-board.svg`` illustrating the three
    medallion layers, row counts, and quality gate results.

    Args:
        proof_pack: The lakehouse proof-pack dictionary.
    """
    logger.info("Generating SVG architecture board")
    summary: dict[str, Any] = proof_pack["summary"]
    expectations: list[dict[str, Any]] = proof_pack["governance"]["expectations"]
    lines: list[str] = [
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>",
        "<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"1280\" height=\"720\" viewBox=\"0 0 1280 720\">",
        "<rect width=\"1280\" height=\"720\" fill=\"#07111f\"/>",
        "<rect x=\"40\" y=\"40\" width=\"1200\" height=\"640\" rx=\"28\" fill=\"#0f1728\" stroke=\"#d7b268\" stroke-width=\"2\"/>",
        "<text x=\"80\" y=\"110\" fill=\"#d7b268\" font-family=\"Georgia,serif\" font-size=\"28\">Lakehouse Contract Lab</text>",
        "<text x=\"80\" y=\"150\" fill=\"#f6f3ec\" font-family=\"Arial,sans-serif\" font-size=\"18\">Spark + Delta medallion pipeline with quality gates and Delta version tracking.</text>",
        f"<text x=\"80\" y=\"210\" fill=\"#f6f3ec\" font-family=\"Arial,sans-serif\" font-size=\"22\">Bronze {summary['bronzeRows']} rows</text>",
        f"<text x=\"440\" y=\"210\" fill=\"#f6f3ec\" font-family=\"Arial,sans-serif\" font-size=\"22\">Silver {summary['silverAcceptedRows']} accepted / {summary['silverRejectedRows']} rejected</text>",
        f"<text x=\"960\" y=\"210\" fill=\"#f6f3ec\" font-family=\"Arial,sans-serif\" font-size=\"22\">Gold {summary['goldRows']} KPI rows</text>",
        "<rect x=\"80\" y=\"260\" width=\"260\" height=\"240\" rx=\"18\" fill=\"#14223a\" stroke=\"#2f4e77\"/>",
        "<rect x=\"400\" y=\"260\" width=\"340\" height=\"240\" rx=\"18\" fill=\"#132b24\" stroke=\"#4e9f7d\"/>",
        "<rect x=\"800\" y=\"260\" width=\"360\" height=\"240\" rx=\"18\" fill=\"#2a2411\" stroke=\"#d7b268\"/>",
        "<text x=\"110\" y=\"310\" fill=\"#dce8ff\" font-family=\"Arial,sans-serif\" font-size=\"24\">Bronze</text>",
        "<text x=\"430\" y=\"310\" fill=\"#ddf7eb\" font-family=\"Arial,sans-serif\" font-size=\"24\">Silver</text>",
        "<text x=\"830\" y=\"310\" fill=\"#fff3d1\" font-family=\"Arial,sans-serif\" font-size=\"24\">Gold</text>",
        "<text x=\"110\" y=\"350\" fill=\"#c7d4e9\" font-family=\"Arial,sans-serif\" font-size=\"18\">Raw order envelope preserved</text>",
        "<text x=\"110\" y=\"382\" fill=\"#c7d4e9\" font-family=\"Arial,sans-serif\" font-size=\"18\">Ingest timestamp attached</text>",
        "<text x=\"110\" y=\"414\" fill=\"#c7d4e9\" font-family=\"Arial,sans-serif\" font-size=\"18\">Delta log version tracked</text>",
        "<text x=\"430\" y=\"350\" fill=\"#caefdd\" font-family=\"Arial,sans-serif\" font-size=\"18\">Customer, region, and amount validated</text>",
        "<text x=\"430\" y=\"382\" fill=\"#caefdd\" font-family=\"Arial,sans-serif\" font-size=\"18\">Latest duplicate survives</text>",
        "<text x=\"430\" y=\"414\" fill=\"#caefdd\" font-family=\"Arial,sans-serif\" font-size=\"18\">Rejected rows retained for review</text>",
        "<text x=\"830\" y=\"350\" fill=\"#fbe9bc\" font-family=\"Arial,sans-serif\" font-size=\"18\">Region KPI contract for platform teams</text>",
        "<text x=\"830\" y=\"382\" fill=\"#fbe9bc\" font-family=\"Arial,sans-serif\" font-size=\"18\">Revenue, completed orders, active pipeline</text>",
        "<text x=\"830\" y=\"414\" fill=\"#fbe9bc\" font-family=\"Arial,sans-serif\" font-size=\"18\">Delta-backed gold table summary</text>",
        "<path d=\"M340 380 L400 380\" stroke=\"#d7b268\" stroke-width=\"4\" marker-end=\"url(#arrow)\"/>",
        "<path d=\"M740 380 L800 380\" stroke=\"#d7b268\" stroke-width=\"4\" marker-end=\"url(#arrow)\"/>",
        "<defs><marker id=\"arrow\" markerWidth=\"10\" markerHeight=\"10\" refX=\"8\" refY=\"3\" orient=\"auto\"><path d=\"M0,0 L0,6 L9,3 z\" fill=\"#d7b268\"/></marker></defs>",
        "<text x=\"80\" y=\"560\" fill=\"#d7b268\" font-family=\"Arial,sans-serif\" font-size=\"18\">Quality gates</text>",
    ]
    y: int = 595
    for expectation in expectations:
        lines.append(
            f"<text x=\"80\" y=\"{y}\" fill=\"#f6f3ec\" font-family=\"Arial,sans-serif\" font-size=\"17\">{expectation['name']}: {expectation['passed']} passed / {expectation['failed']} failed</text>"
        )
        y += 28
    lines.extend(
        [
            "</svg>",
        ]
    )
    (DOCS_DIR / "lakehouse-contract-board.svg").write_text(
        "\n".join(lines), encoding="utf-8"
    )
    logger.info("SVG board written to docs/lakehouse-contract-board.svg")


def main() -> None:
    """Execute the full medallion pipeline and generate all artifacts.

    Steps:
        1. Build a local Spark session with Delta support.
        2. Ingest source rows into the bronze layer.
        3. Apply quality gates and deduplication to produce silver.
        4. Aggregate silver into gold-level region KPIs.
        5. Write Delta tables and JSON artifact files.
        6. Generate the SVG architecture board.
    """
    logger.info("Starting lakehouse artifact build")
    if not java_runtime_available():
        validate_prebuilt_artifacts()
        return

    spark: SparkSession = build_spark()
    spark.sparkContext.setLogLevel("ERROR")

    if DELTA_DIR.exists():
        shutil.rmtree(DELTA_DIR)
        logger.info("Cleaned previous Delta directory")
    DELTA_DIR.mkdir(parents=True, exist_ok=True)
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    DOCS_DIR.mkdir(parents=True, exist_ok=True)

    # --- Bronze layer: raw ingestion ---
    logger.info("Building bronze layer from %d source rows", len(SOURCE_ROWS))
    source: DataFrame = (
        spark.createDataFrame(SOURCE_ROWS)
        .withColumn("ingested_at", F.to_timestamp(F.lit(NOW.isoformat())))
        .withColumn("source_rank", F.monotonically_increasing_id())
        .withColumn("order_ts", F.to_timestamp("order_ts"))
    )

    bronze: DataFrame = source.select(
        "order_id",
        "customer_id",
        "region",
        "channel",
        "status",
        "amount",
        "currency",
        "order_ts",
        "ingested_at",
        "source_rank",
    )

    # --- Silver layer: quality gates + dedup ---
    logger.info("Applying quality gates and deduplication for silver layer")
    rule = (
        F.when(F.col("customer_id").isNull(), F.lit("missing_customer"))
        .when(F.col("region").isNull(), F.lit("missing_region"))
        .when(F.col("amount") <= 0, F.lit("non_positive_amount"))
        .otherwise(F.lit(None))
    )
    window = Window.partitionBy("order_id").orderBy(
        F.col("order_ts").desc(), F.col("source_rank").desc()
    )
    staged: DataFrame = (
        bronze.withColumn("quality_issue", rule)
        .withColumn("row_rank", F.row_number().over(window))
        .withColumn(
            "rejection_reason",
            F.when(F.col("quality_issue").isNotNull(), F.col("quality_issue"))
            .when(F.col("row_rank") > 1, F.lit("stale_duplicate"))
            .otherwise(F.lit(None)),
        )
    )
    silver: DataFrame = staged.filter(F.col("rejection_reason").isNull())
    rejected: DataFrame = staged.filter(F.col("rejection_reason").isNotNull())

    # --- Gold layer: region KPI aggregation ---
    logger.info("Aggregating gold-layer region KPIs")
    gold: DataFrame = (
        silver.groupBy("region")
        .agg(
            F.round(F.sum("amount"), 2).alias("gross_revenue_usd"),
            F.count("*").alias("accepted_orders"),
            F.sum(F.when(F.col("status") == "completed", 1).otherwise(0)).alias(
                "completed_orders"
            ),
            F.sum(F.when(F.col("status") == "processing", 1).otherwise(0)).alias(
                "pipeline_orders"
            ),
            F.countDistinct("customer_id").alias("distinct_customers"),
        )
        .orderBy("region")
    )

    # --- Write Delta tables ---
    table_specs: dict[str, tuple[DataFrame, Path]] = {
        "bronze": (bronze, DELTA_DIR / "bronze_orders"),
        "silver": (silver, DELTA_DIR / "silver_orders"),
        "gold": (gold, DELTA_DIR / "gold_region_kpis"),
    }
    for layer_name, (df, path) in table_specs.items():
        df.write.format("delta").mode("overwrite").save(str(path))
        logger.info("Delta table written: %s -> %s", layer_name, path.name)

    # --- Compute metrics ---
    bronze_count: int = bronze.count()
    silver_count: int = silver.count()
    rejected_count: int = rejected.count()
    gold_count: int = gold.count()
    pass_rate: float = round((silver_count / bronze_count) * 100, 2)

    logger.info(
        "Pipeline metrics: bronze=%d silver=%d rejected=%d gold=%d pass_rate=%.2f%%",
        bronze_count, silver_count, rejected_count, gold_count, pass_rate,
    )

    expectations: list[dict[str, Any]] = [
        {
            "name": "customer_present",
            "layer": "silver",
            "passed": bronze.filter(F.col("customer_id").isNotNull()).count(),
            "failed": bronze.filter(F.col("customer_id").isNull()).count(),
            "rule": "customer_id must be present before rows graduate into silver.",
        },
        {
            "name": "region_present",
            "layer": "silver",
            "passed": bronze.filter(F.col("region").isNotNull()).count(),
            "failed": bronze.filter(F.col("region").isNull()).count(),
            "rule": "region is required for downstream territory KPIs.",
        },
        {
            "name": "positive_amount",
            "layer": "silver",
            "passed": bronze.filter(F.col("amount") > 0).count(),
            "failed": bronze.filter(F.col("amount") <= 0).count(),
            "rule": "non-positive revenue is isolated into rejected review rows.",
        },
        {
            "name": "latest_order_record",
            "layer": "silver",
            "passed": silver_count,
            "failed": staged.filter(F.col("row_rank") > 1).count(),
            "rule": "only the latest duplicate order version can move into silver.",
        },
    ]

    # --- Build artifacts ---
    proof_pack: dict[str, Any] = {
        "service": "lakehouse-contract-lab",
        "status": "ok",
        "generatedAt": NOW.isoformat(),
        "schema": "lakehouse-proof-pack-v1",
        "headline": (
            "Spark + Delta medallion proof with explicit contract boundaries, "
            "quality gates, and Delta version tracking."
        ),
        "summary": {
            "sourceRows": len(SOURCE_ROWS),
            "bronzeRows": bronze_count,
            "silverAcceptedRows": silver_count,
            "silverRejectedRows": rejected_count,
            "goldRows": gold_count,
            "deltaTables": 3,
            "qualityPassRatePct": pass_rate,
        },
        "resourcePack": {
            **resource_pack_summary(),
            "qualityRules": load_quality_rules(),
            "exportTargets": load_export_targets(),
            "validationCases": load_validation_cases(),
        },
        "tables": [
            {
                "layer": "bronze",
                "tableName": "bronze_orders",
                "deltaVersion": latest_delta_version(DELTA_DIR / "bronze_orders"),
                "rows": bronze_count,
                "contract": [
                    "raw order envelope preserved",
                    "ingested_at attached",
                    "source_rank retained for duplicate review",
                ],
            },
            {
                "layer": "silver",
                "tableName": "silver_orders",
                "deltaVersion": latest_delta_version(DELTA_DIR / "silver_orders"),
                "rows": silver_count,
                "contract": [
                    "customer and region required",
                    "amount must stay positive",
                    "latest duplicate only",
                ],
            },
            {
                "layer": "gold",
                "tableName": "gold_region_kpis",
                "deltaVersion": latest_delta_version(DELTA_DIR / "gold_region_kpis"),
                "rows": gold_count,
                "contract": [
                    "region-level KPI output",
                    "completed and pipeline counts explicit",
                    "distinct customer count preserved",
                ],
            },
        ],
        "governance": {
            "approvalBoundary": (
                "Only silver-accepted rows can shape gold KPIs; rejected rows stay "
                "queryable for human review."
            ),
            "expectations": expectations,
            "rejectedReasons": [
                row["rejection_reason"]
                for row in rows_to_json(
                    rejected.select("rejection_reason").distinct(),
                    ["rejection_reason"],
                    limit=10,
                )
            ],
        },
        "snowflakeFit": {
            "whyItMatters": (
                "Shows contract-first medallion thinking, governed KPI outputs, and "
                "handoff-friendly review assets for solution engineering conversations."
            ),
            "reviewPath": [
                "/api/runtime/lakehouse-proof-pack",
                "/api/runtime/table-preview/gold",
                "docs/lakehouse-contract-board.svg",
            ],
        },
        "databricksFit": {
            "whyItMatters": (
                "Uses real Spark + Delta execution and exposes quality gates, Delta "
                "versions, and medallion transitions as explicit public proof."
            ),
            "reviewPath": [
                "/api/runtime/lakehouse-proof-pack",
                "/api/runtime/quality-report",
                "scripts/build_lakehouse_artifacts.py",
            ],
        },
        "proofAssets": [
            "artifacts/lakehouse-proof-pack.json",
            "artifacts/quality-report.json",
            "docs/lakehouse-contract-board.svg",
            "scripts/build_lakehouse_artifacts.py",
            *[f"data/{path.name}" for path in data_files().values()],
        ],
        "links": {
            "health": "/health",
            "proofPack": "/api/runtime/lakehouse-proof-pack",
            "qualityReport": "/api/runtime/quality-report",
            "bronzePreview": "/api/runtime/table-preview/bronze",
            "silverPreview": "/api/runtime/table-preview/silver",
            "goldPreview": "/api/runtime/table-preview/gold",
        },
    }

    quality_report: dict[str, Any] = {
        "service": "lakehouse-contract-lab",
        "generatedAt": NOW.isoformat(),
        "schema": "lakehouse-quality-report-v1",
        "summary": {
            "acceptedRows": silver_count,
            "failedRows": rejected_count,
            "qualityPassRatePct": pass_rate,
        },
        "expectations": expectations,
        "rejectedPreview": rows_to_json(
            rejected.select(
                "order_id",
                "customer_id",
                "region",
                "amount",
                "rejection_reason",
            ),
            ["order_id"],
            limit=10,
        ),
    }

    bronze_preview: dict[str, Any] = {
        "schema": "lakehouse-table-preview-v1",
        "layer": "bronze",
        "generatedAt": NOW.isoformat(),
        "rows": rows_to_json(bronze, ["order_id"], limit=5),
    }
    silver_preview: dict[str, Any] = {
        "schema": "lakehouse-table-preview-v1",
        "layer": "silver",
        "generatedAt": NOW.isoformat(),
        "rows": rows_to_json(
            silver.select(
                "order_id",
                "customer_id",
                "region",
                "status",
                "amount",
                "currency",
            ),
            ["order_id"],
            limit=5,
        ),
    }
    gold_preview: dict[str, Any] = {
        "schema": "lakehouse-table-preview-v1",
        "layer": "gold",
        "generatedAt": NOW.isoformat(),
        "rows": rows_to_json(gold, ["region"], limit=10),
    }

    write_json(ARTIFACTS_DIR / "lakehouse-proof-pack.json", proof_pack)
    write_json(ARTIFACTS_DIR / "quality-report.json", quality_report)
    write_json(ARTIFACTS_DIR / "review-summary.json", build_review_summary_artifact(proof_pack, quality_report))
    write_json(
        ARTIFACTS_DIR / "source-pack.json",
        {
            "schema": "lakehouse-source-pack-v1",
            "generatedAt": NOW.isoformat(),
            "summary": resource_pack_summary(),
            "sourceRows": SOURCE_ROWS,
            "qualityRules": load_quality_rules(),
            "exportTargets": load_export_targets(),
            "validationCases": load_validation_cases(),
        },
    )
    write_json(ARTIFACTS_DIR / "bronze-preview.json", bronze_preview)
    write_json(ARTIFACTS_DIR / "silver-preview.json", silver_preview)
    write_json(ARTIFACTS_DIR / "gold-preview.json", gold_preview)
    build_svg(proof_pack)

    spark.stop()
    logger.info("Lakehouse artifact build complete")


if __name__ == "__main__":
    main()
