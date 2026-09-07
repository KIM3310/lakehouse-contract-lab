"""Build and independently reload actual Delta tables; never substitutes JSON snapshots."""

from __future__ import annotations

import hashlib
import json
import platform
from importlib.metadata import version
from pathlib import Path

from scripts import build_lakehouse_artifacts as pipeline


def main() -> None:
    pipeline.main()
    spark = pipeline.build_spark()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        tables = {}
        for name, directory in [("bronze", "bronze_orders"), ("silver", "silver_orders"), ("gold", "gold_region_kpis")]:
            location = pipeline.DELTA_DIR / directory
            frame = spark.read.format("delta").load(str(location))
            tables[name] = {"rows": frame.count(), "deltaVersion": pipeline.latest_delta_version(location)}
            assert tables[name]["deltaVersion"] == 0
        assert {name: data["rows"] for name, data in tables.items()} == {"bronze": 12, "silver": 8, "gold": 5}
        silver = spark.read.format("delta").load(str(pipeline.DELTA_DIR / "silver_orders"))
        assert silver.filter("customer_id IS NULL OR region IS NULL OR amount <= 0").count() == 0
        assert silver.select("order_id").distinct().count() == 8
        proof = {
            "schema": "lakehouse-actual-runtime-v1",
            "scope": "local[2], synthetic 12-row source fixture; actual Spark and Delta read/write, no warehouse export",
            "python": platform.python_version(),
            "spark": version("pyspark"),
            "delta": version("delta-spark"),
            "java": spark.sparkContext._jvm.java.lang.System.getProperty("java.version"),
            "tables": tables,
            "artifacts": {
                path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                for path in sorted(pipeline.ARTIFACTS_DIR.glob("*-preview.json"))
            },
            "pipelineSha256": hashlib.sha256(Path(pipeline.__file__).read_bytes()).hexdigest(),
        }
        output = pipeline.ROOT / "docs/evidence/spark-runtime.json"
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(proof, indent=2) + "\n")
        print(json.dumps(proof, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
