# Build stage
FROM python:3.14-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:0.9 /uv /usr/local/bin/uv

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY s3proxy/ ./s3proxy/
RUN uv sync --frozen --no-dev


# Runtime stage
FROM python:3.14-slim

RUN useradd --create-home --shell /bin/bash s3proxy

WORKDIR /app

COPY --from=builder --chown=s3proxy:s3proxy /app/.venv /app/.venv
COPY --from=builder --chown=s3proxy:s3proxy /app/s3proxy /app/s3proxy

USER s3proxy

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

ENV S3PROXY_IP=0.0.0.0 \
    S3PROXY_PORT=4433 \
    S3PROXY_NO_TLS=true \
    S3PROXY_REGION=us-east-1 \
    S3PROXY_LOG_LEVEL=INFO

EXPOSE 4433

CMD ["uvicorn", "s3proxy.main:app", "--host", "0.0.0.0", "--port", "4433", "--loop", "uvloop"]
