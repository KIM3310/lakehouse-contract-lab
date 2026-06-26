# =============================================================================
# Lakehouse Contract Lab - Makefile
# =============================================================================

.PHONY: check-bootstrap-python install test lint format build smoke smoke-no-build verify docker-build docker-run docker-down pipeline clean help

VENV   := .venv
PYTHON_MIN_VERSION := 3.11
PYTHON_CANDIDATES := python3.13 python3.12 python3.11 python3
BOOTSTRAP_PYTHON ?= $(shell for py in $(PYTHON_CANDIDATES); do \
	if command -v $$py >/dev/null 2>&1 && $$py -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 11) else 1)' >/dev/null 2>&1; then \
		command -v $$py; \
		break; \
	fi; \
done)
VENV_PY := $(VENV)/bin/python
PIP    := $(VENV)/bin/pip

APP_PORT ?= 8096
IMAGE_NAME ?= lakehouse-contract-lab
IMAGE_TAG  ?= latest

help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

check-bootstrap-python:
	@if [ -z "$(BOOTSTRAP_PYTHON)" ]; then \
		echo "Python $(PYTHON_MIN_VERSION)+ is required." >&2; \
		echo "Install Python $(PYTHON_MIN_VERSION)+ or run: make BOOTSTRAP_PYTHON=/path/to/python$(PYTHON_MIN_VERSION) <target>" >&2; \
		exit 1; \
	fi
	@$(BOOTSTRAP_PYTHON) -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 11) else 1)' || { \
		echo "BOOTSTRAP_PYTHON=$(BOOTSTRAP_PYTHON) is not Python $(PYTHON_MIN_VERSION)+." >&2; \
		exit 1; \
	}

install: check-bootstrap-python ## Create venv and install all dependencies
	@if [ ! -x "$(VENV_PY)" ] || ! $(VENV_PY) -c "import sys; raise SystemExit(0 if sys.version_info >= (3, 11) else 1)" >/dev/null 2>&1; then \
		rm -rf $(VENV); \
		$(BOOTSTRAP_PYTHON) -m venv $(VENV); \
	fi
	@if ! $(VENV_PY) -m pip --version >/dev/null 2>&1; then \
		$(VENV_PY) -m ensurepip --upgrade; \
	fi
	$(VENV_PY) -m pip install --upgrade pip
	$(VENV_PY) -m pip install -e ".[dev]"

test: install ## Run the pytest suite
	$(VENV_PY) -m pytest -v --tb=short

test-cov: install ## Run tests with coverage reporting
	$(VENV_PY) -m pytest -v --tb=short --cov=app --cov=scripts --cov-report=term-missing --cov-report=html

lint: install ## Run ruff linter
	$(VENV_PY) -m ruff check .

format: install ## Run ruff formatter
	$(VENV_PY) -m ruff format .

format-check: install ## Check formatting without modifying files
	$(VENV_PY) -m ruff format --check .

build: install ## Run the medallion pipeline and generate artifacts
	$(VENV_PY) scripts/build_lakehouse_artifacts.py

smoke: build ## Boot local API and smoke key runtime surfaces
	@$(MAKE) smoke-no-build

smoke-no-build: install ## Boot local API and smoke key runtime surfaces without rebuilding artifacts
	@set -eu; \
	PORT=8097; \
	LOG=/tmp/lakehouse-contract-lab-smoke.log; \
	$(VENV_PY) -m uvicorn app.main:app --host 127.0.0.1 --port $$PORT >$$LOG 2>&1 & \
	pid=$$!; \
	trap 'kill $$pid >/dev/null 2>&1 || true' EXIT INT TERM; \
	for _ in 1 2 3 4 5 6 7 8 9 10; do \
		if curl -fsS "http://127.0.0.1:$$PORT/health" >/dev/null 2>&1; then \
			break; \
		fi; \
		sleep 1; \
	done; \
	curl -fsS "http://127.0.0.1:$$PORT/health" >/dev/null; \
	curl -fsS "http://127.0.0.1:$$PORT/api/runtime/quality-report" >/dev/null; \
	echo "smoke ok: http://127.0.0.1:$$PORT"

verify: pipeline smoke-no-build ## Full local verification including artifact build and API smoke

serve: install ## Start the FastAPI development server
	$(VENV_PY) -m uvicorn app.main:app --host 127.0.0.1 --port $(APP_PORT) --reload

docker-build: ## Build the Docker image
	docker build -t $(IMAGE_NAME):$(IMAGE_TAG) .

docker-run: ## Run the app via Docker Compose
	docker compose up --build -d

docker-down: ## Stop Docker Compose services
	docker compose down

docker-logs: ## Follow Docker Compose logs
	docker compose logs -f app

pipeline: lint test build ## Full pipeline: lint, test, then build artifacts

clean: ## Remove generated files and caches
	rm -rf $(VENV) __pycache__ .pytest_cache .ruff_cache *.egg-info
	rm -rf artifacts/runtime_delta
	rm -rf htmlcov .coverage
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
