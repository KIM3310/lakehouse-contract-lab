# =============================================================================
# Lakehouse Contract Lab - Makefile
# =============================================================================

.PHONY: install test lint format build smoke verify docker-build docker-run docker-down pipeline clean help

VENV   := .venv
VENV_PY := $(VENV)/bin/python
PYTHON ?= python3
ifeq ($(wildcard $(VENV_PY)), $(VENV_PY))
PYTHON := $(VENV_PY)
endif
PIP    := $(VENV)/bin/pip
PYTEST := $(VENV)/bin/pytest
RUFF   := $(VENV)/bin/ruff
UVICORN := $(VENV)/bin/uvicorn

APP_PORT ?= 8096
IMAGE_NAME ?= lakehouse-contract-lab
IMAGE_TAG  ?= latest

help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

install: ## Create venv and install all dependencies
	$(PYTHON) -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -e ".[dev]"

test: ## Run the pytest suite
	$(PYTEST) -v --tb=short

test-cov: ## Run tests with coverage reporting
	$(PYTEST) -v --tb=short --cov=app --cov=scripts --cov-report=term-missing --cov-report=html

lint: ## Run ruff linter
	$(RUFF) check .

format: ## Run ruff formatter
	$(RUFF) format .

format-check: ## Check formatting without modifying files
	$(RUFF) format --check .

build: ## Run the medallion pipeline and generate artifacts
	$(PYTHON) scripts/build_lakehouse_artifacts.py

smoke: build ## Boot local API and smoke key runtime surfaces
	@set -eu; \
	PORT=8097; \
	LOG=/tmp/lakehouse-contract-lab-smoke.log; \
	$(UVICORN) app.main:app --host 127.0.0.1 --port $$PORT >$$LOG 2>&1 & \
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

verify: pipeline smoke ## Full local verification including artifact build and API smoke

serve: ## Start the FastAPI development server
	$(UVICORN) app.main:app --host 127.0.0.1 --port $(APP_PORT) --reload

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
