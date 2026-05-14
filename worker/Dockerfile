FROM python:3.15-slim AS builder

WORKDIR /build

# Install build tools needed for some native Python extensions
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        libffi-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --upgrade pip \
    && pip install --no-cache-dir --prefix=/install -r requirements.txt

FROM python:3.15-slim AS runtime

RUN useradd --create-home --uid 1000 worker

WORKDIR /app

COPY --from=builder /install /usr/local

COPY worker.py .
COPY entrypoint.sh .

RUN mkdir -p /app/config && chown -R worker:worker /app

RUN chmod +x /app/entrypoint.sh

USER worker

# Runtime environment defaults (all can be overridden at pod level)
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    LOG_LEVEL=INFO \
    JOB_CONFIG_PATH=/app/config/job_config.yaml

ENTRYPOINT ["/app/entrypoint.sh"]

