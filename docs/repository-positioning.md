# Lakehouse Contract Lab in Selected Work

Updated 2026-09-07.

This project adds **data engineering · quality gates · reproducible execution** to the portfolio. Selection is based on distinct, inspectable implementation rather than a particular employment role or startup category.

99 unit/API tests pass. Actual local Spark 3.5.9 + Delta 3.3.3 execution writes and reloads 12 bronze, 8 silver and 5 gold rows; 4 source rows are rejected. The runtime report records versions and source/artifact hashes.

This is a synthetic 12-row fixture on local[2], not a scale benchmark. Snowflake/Databricks exports and cloud infrastructure were not executed in this verification. LAKEHOUSE_VALIDATE_PREBUILT_ONLY=1 validates snapshots only and is explicitly reported as such.

[Implementation entry points](../README.md) · [Verification](VERIFICATION.md)
