# =============================================================================
# Lakehouse Contract Lab - Production Container
# Python 3.11 + Java 17 (for Spark/Delta) on slim Debian
# =============================================================================

FROM python:3.11-slim AS base

LABEL maintainer="Lakehouse Contract Lab"
LABEL description="Spark + Delta medallion pipeline with quality gates and FastAPI"

# Prevent Python from writing .pyc files and enable unbuffered stdout/stderr
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Install Java 17 (required by Spark) and curl (for healthcheck)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        curl \
        procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"
ENV SPARK_LOCAL_IP=127.0.0.1

WORKDIR /app

# Install Python dependencies first (layer caching)
COPY requirements.txt pyproject.toml ./
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app/ ./app/
COPY scripts/ ./scripts/
COPY artifacts/ ./artifacts/
COPY docs/ ./docs/

# Expose the API port
EXPOSE 8096

# Healthcheck against the FastAPI /health endpoint
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:8096/health || exit 1

# Default: rebuild artifacts then start the API server
CMD ["sh", "-c", "python scripts/build_lakehouse_artifacts.py && uvicorn app.main:app --host 0.0.0.0 --port 8096"]
