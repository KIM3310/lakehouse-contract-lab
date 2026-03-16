from __future__ import annotations

import json
import os
import shutil
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from delta import configure_spark_with_delta_pip
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[1]
ARTIFACTS_DIR = ROOT / "artifacts"
DELTA_DIR = ARTIFACTS_DIR / "runtime_delta"
DOCS_DIR = ROOT / "docs"

NOW = datetime.now(timezone.utc).replace(microsecond=0)

SOURCE_ROWS = [
    {
        "order_id": "O-1001",
        "customer_id": "C-001",
        "region": "KR-SEOUL",
        "channel": "web",
        "status": "completed",
        "amount": 180.50,
        "currency": "USD",
        "order_ts": "2026-03-14T09:00:00Z",
    },
    {
        "order_id": "O-1002",
        "customer_id": "C-002",
        "region": "KR-BUSAN",
        "channel": "partner",
        "status": "processing",
        "amount": 220.00,
        "currency": "USD",
        "order_ts": "2026-03-14T09:20:00Z",
    },
    {
        "order_id": "O-1003",
        "customer_id": "C-003",
        "region": "JP-TOKYO",
        "channel": "field",
        "status": "completed",
        "amount": 510.25,
        "currency": "USD",
        "order_ts": "2026-03-14T09:25:00Z",
    },
    {
        "order_id": "O-1004",
        "customer_id": "C-004",
        "region": "US-WEST",
        "channel": "web",
        "status": "completed",
        "amount": 305.10,
        "currency": "USD",
        "order_ts": "2026-03-14T10:00:00Z",
    },
    {
        "order_id": "O-1005",
        "customer_id": "C-005",
        "region": "US-WEST",
        "channel": "web",
        "status": "cancelled",
        "amount": 80.00,
        "currency": "USD",
        "order_ts": "2026-03-14T10:05:00Z",
    },
    {
        "order_id": "O-1006",
        "customer_id": "C-006",
        "region": "DE-BERLIN",
        "channel": "field",
        "status": "completed",
        "amount": 410.75,
        "currency": "USD",
        "order_ts": "2026-03-14T10:25:00Z",
    },
    {
        "order_id": "O-1007",
        "customer_id": "C-007",
        "region": "KR-SEOUL",
        "channel": "partner",
        "status": "processing",
        "amount": 260.40,
        "currency": "USD",
        "order_ts": "2026-03-14T10:35:00Z",
    },
    {
        "order_id": "O-1008",
        "customer_id": None,
        "region": "KR-SEOUL",
        "channel": "web",
        "status": "completed",
        "amount": 330.00,
        "currency": "USD",
        "order_ts": "2026-03-14T11:00:00Z",
    },
    {
        "order_id": "O-1009",
        "customer_id": "C-009",
        "region": None,
        "channel": "partner",
        "status": "completed",
        "amount": 190.00,
        "currency": "USD",
        "order_ts": "2026-03-14T11:12:00Z",
    },
    {
        "order_id": "O-1010",
        "customer_id": "C-010",
        "region": "JP-TOKYO",
        "channel": "field",
        "status": "completed",
        "amount": -15.00,
        "currency": "USD",
        "order_ts": "2026-03-14T11:20:00Z",
    },
    {
        "order_id": "O-1002",
        "customer_id": "C-002",
        "region": "KR-BUSAN",
        "channel": "partner",
        "status": "completed",
        "amount": 220.00,
        "currency": "USD",
        "order_ts": "2026-03-14T12:20:00Z",
    },
    {
        "order_id": "O-1011",
        "customer_id": "C-011",
        "region": "US-WEST",
        "channel": "web",
        "status": "processing",
        "amount": 125.25,
        "currency": "USD",
        "order_ts": "2026-03-14T12:30:00Z",
    },
]


def ensure_java_home() -> None:
    if os.environ.get("JAVA_HOME"):
        os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
        return
    java_home = Path("/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home")
    if java_home.exists():
        os.environ["JAVA_HOME"] = str(java_home)
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")


def build_spark() -> SparkSession:
    ensure_java_home()
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
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return value


def rows_to_json(df: DataFrame, order_by: list[str], limit: int = 5) -> list[dict[str, Any]]:
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
    log_dir = table_path / "_delta_log"
    versions = [
        int(path.stem)
        for path in log_dir.glob("*.json")
        if path.stem.isdigit()
    ]
    return max(versions) if versions else None


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def build_svg(proof_pack: dict[str, Any]) -> None:
    summary = proof_pack["summary"]
    expectations = proof_pack["governance"]["expectations"]
    lines = [
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>",
        "<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"1280\" height=\"720\" viewBox=\"0 0 1280 720\">",
        "<rect width=\"1280\" height=\"720\" fill=\"#07111f\"/>",
        "<rect x=\"40\" y=\"40\" width=\"1200\" height=\"640\" rx=\"28\" fill=\"#0f1728\" stroke=\"#d7b268\" stroke-width=\"2\"/>",
        "<text x=\"80\" y=\"110\" fill=\"#d7b268\" font-family=\"Georgia,serif\" font-size=\"28\">Lakehouse Contract Lab</text>",
        "<text x=\"80\" y=\"150\" fill=\"#f6f3ec\" font-family=\"Arial,sans-serif\" font-size=\"18\">Spark + Delta medallion proof with quality gates, Delta versions, and reviewer-fast platform posture.</text>",
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
    y = 595
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


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("ERROR")
    if DELTA_DIR.exists():
        shutil.rmtree(DELTA_DIR)
    DELTA_DIR.mkdir(parents=True, exist_ok=True)
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    DOCS_DIR.mkdir(parents=True, exist_ok=True)

    source = (
        spark.createDataFrame(SOURCE_ROWS)
        .withColumn("ingested_at", F.to_timestamp(F.lit(NOW.isoformat())))
        .withColumn("source_rank", F.monotonically_increasing_id())
        .withColumn("order_ts", F.to_timestamp("order_ts"))
    )

    bronze = source.select(
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

    rule = (
        F.when(F.col("customer_id").isNull(), F.lit("missing_customer"))
        .when(F.col("region").isNull(), F.lit("missing_region"))
        .when(F.col("amount") <= 0, F.lit("non_positive_amount"))
        .otherwise(F.lit(None))
    )
    window = Window.partitionBy("order_id").orderBy(
        F.col("order_ts").desc(), F.col("source_rank").desc()
    )
    staged = (
        bronze.withColumn("quality_issue", rule)
        .withColumn("row_rank", F.row_number().over(window))
        .withColumn(
            "rejection_reason",
            F.when(F.col("quality_issue").isNotNull(), F.col("quality_issue"))
            .when(F.col("row_rank") > 1, F.lit("stale_duplicate"))
            .otherwise(F.lit(None)),
        )
    )
    silver = staged.filter(F.col("rejection_reason").isNull())
    rejected = staged.filter(F.col("rejection_reason").isNotNull())
    gold = (
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

    table_specs = {
        "bronze": (bronze, DELTA_DIR / "bronze_orders"),
        "silver": (silver, DELTA_DIR / "silver_orders"),
        "gold": (gold, DELTA_DIR / "gold_region_kpis"),
    }
    for _, (df, path) in table_specs.items():
        df.write.format("delta").mode("overwrite").save(str(path))

    bronze_count = bronze.count()
    silver_count = silver.count()
    rejected_count = rejected.count()
    gold_count = gold.count()
    pass_rate = round((silver_count / bronze_count) * 100, 2)

    expectations = [
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

    proof_pack = {
        "service": "lakehouse-contract-lab",
        "status": "ok",
        "generatedAt": NOW.isoformat(),
        "schema": "lakehouse-proof-pack-v1",
        "headline": (
            "Spark + Delta medallion proof with explicit contract boundaries, "
            "quality gates, and reviewer-fast platform posture."
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

    quality_report = {
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

    bronze_preview = {
        "schema": "lakehouse-table-preview-v1",
        "layer": "bronze",
        "generatedAt": NOW.isoformat(),
        "rows": rows_to_json(bronze, ["order_id"], limit=5),
    }
    silver_preview = {
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
    gold_preview = {
        "schema": "lakehouse-table-preview-v1",
        "layer": "gold",
        "generatedAt": NOW.isoformat(),
        "rows": rows_to_json(gold, ["region"], limit=10),
    }

    write_json(ARTIFACTS_DIR / "lakehouse-proof-pack.json", proof_pack)
    write_json(ARTIFACTS_DIR / "quality-report.json", quality_report)
    write_json(ARTIFACTS_DIR / "bronze-preview.json", bronze_preview)
    write_json(ARTIFACTS_DIR / "silver-preview.json", silver_preview)
    write_json(ARTIFACTS_DIR / "gold-preview.json", gold_preview)
    build_svg(proof_pack)
    spark.stop()


if __name__ == "__main__":
    main()

