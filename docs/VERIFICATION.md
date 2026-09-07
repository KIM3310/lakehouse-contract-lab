# Lakehouse Contract Lab — implementation and verification

Reviewed 2026-09-07. The repository and linked tests define the evidence; a preview alone does not establish production readiness.

### Missing Java is a failure

A normal build can no longer silently substitute old JSON artifacts. Snapshot-only verification requires an explicit flag, including in CI.

### Serialize timestamps inside Spark

UTC timestamp formatting happens before Python collection, avoiding a nine-hour drift on a Seoul workstation.

### Reload, then verify

A second Spark session reads the real Delta logs/tables and checks counts, uniqueness and quality. Pipeline sessions close in finally blocks.

## Reproduce

```sh
make install
# Java 17 must be available; set JAVA_HOME when needed.
.venv/bin/python -m scripts.verify_spark_runtime
make verify
```

99 unit/API tests pass. Actual local Spark 3.5.9 + Delta 3.3.3 execution writes and reloads 12 bronze, 8 silver and 5 gold rows; 4 source rows are rejected. The runtime report records versions and source/artifact hashes.

## Boundaries

This is a synthetic 12-row fixture on local[2], not a scale benchmark. Snowflake/Databricks exports and cloud infrastructure were not executed in this verification. LAKEHOUSE_VALIDATE_PREBUILT_ONLY=1 validates snapshots only and is explicitly reported as such.

## Attribution

This page describes capabilities visible in the repository. It does not independently establish which lines were written manually, with AI assistance, or by collaborators. The commit history and pull-request diffs preserve the implementation trail; individual/team contribution percentages have not been inferred.

[Back to the project](../README.md)
