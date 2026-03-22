# Contributing to Lakehouse Contract Lab

Thank you for your interest in contributing. This guide covers the development setup and conventions used in this project.

## Prerequisites

- Python 3.11+
- Java 17 (for Spark/Delta pipeline execution; optional for test-only workflows)
- Docker (optional, for containerized runs)

## Development Setup

```bash
git clone https://github.com/KIM3310/lakehouse-contract-lab.git
cd lakehouse-contract-lab
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

## Running Tests

```bash
make test           # Run the pytest suite
make test-cov       # Run with coverage reporting
```

Tests are written with `pytest` and located in `tests/`. The test suite mocks out PySpark and Delta Lake so that most tests run without a Java runtime.

## Linting and Formatting

This project uses [Ruff](https://github.com/astral-sh/ruff) for linting and formatting.

```bash
make lint           # Check for lint issues
make format         # Auto-format code
make format-check   # Check formatting without modifying files
```

Configuration lives in `pyproject.toml` under `[tool.ruff]`:
- Target: Python 3.11
- Line length: 120 characters
- Selected rules: `E`, `F`, `W`, `I`, `B`, `SIM`

## Building Artifacts

```bash
make build          # Run the full Spark + Delta medallion pipeline
```

This requires a Java 17 runtime. On machines without Java, the build script validates the checked-in prebuilt artifacts instead.

## Code Style

- Use type annotations on all public function signatures.
- Write docstrings for public modules, classes, and functions.
- Follow existing patterns in `app/` and `scripts/` for naming and structure.
- Keep cloud integrations env-var gated so the pipeline runs fully locally by default.

## Adding a New Quality Rule

1. Add the rule definition to `data/quality_rules.json`.
2. Add the corresponding `WHEN` clause in `scripts/build_lakehouse_artifacts.py` inside the silver-layer quality gate chain.
3. Add a test row to `data/source_orders.csv` that exercises the new rule.
4. Add a validation case to `data/validation_cases.json`.
5. Update the quality gates table in `README.md`.

## Adding a New Export Target

1. Create an adapter module in `app/` following the pattern of `snowflake_adapter.py` or `databricks_adapter.py`.
2. Gate activation on environment variables so the pipeline works without the target configured.
3. Add the export target metadata to `data/export_targets.json`.
4. Write tests in `tests/` that mock the external SDK.

## Pull Requests

- Branch from `main`.
- Ensure `make pipeline` passes (lint, test, build).
- Keep commits focused; one logical change per commit.
- Update documentation if your change affects the public API or pipeline behavior.

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
