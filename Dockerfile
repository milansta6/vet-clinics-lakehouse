# Reproducible local environment: Python 3.11 + Java 17 + PySpark + Delta Lake.
# Build:  docker build -t vet-lakehouse .
# Run:    docker run --rm vet-lakehouse            (full pipeline)
#         docker run --rm vet-lakehouse make test  (lint + tests)
FROM python:3.11-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless make procps \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Install dependencies first so this layer is cached when only code changes.
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir -e ".[dev]"

# Download the Delta Lake jars once at build time (so runs work without waiting).
RUN python -c "from vet_lakehouse.spark import get_spark; get_spark(local_root='/tmp/warmup', persistent_catalog=False).stop()"

COPY . .

CMD ["make", "run"]
