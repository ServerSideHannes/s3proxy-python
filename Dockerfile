# Build stage - install dependencies
FROM python:3.13-slim AS builder

# Install uv for fast package management
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Copy dependency files first for better layer caching
COPY pyproject.toml uv.lock ./

# Install dependencies into a virtual environment
RUN uv sync --frozen --no-dev --no-install-project

# Copy application code
COPY s3proxy/ ./s3proxy/

# Install the project itself
RUN uv sync --frozen --no-dev


# Runtime stage - minimal image
FROM python:3.13-slim

# Install curl for healthchecks
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd --create-home --shell /bin/bash s3proxy

WORKDIR /app

# Copy virtual environment from builder
COPY --from=builder /app/.venv /app/.venv

# Copy application code
COPY --from=builder /app/s3proxy /app/s3proxy

# Set ownership
RUN chown -R s3proxy:s3proxy /app

USER s3proxy

# Add venv to path
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Default environment variables
ENV S3PROXY_IP=0.0.0.0
ENV S3PROXY_PORT=4433
ENV S3PROXY_NO_TLS=true
ENV S3PROXY_REGION=us-east-1
ENV S3PROXY_LOG_LEVEL=INFO

EXPOSE 4433

# Health check
HEALTHCHECK --interval=5s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:4433/healthz || exit 1

# Run the application with uvloop for better async performance
# Note: Multiple workers require external state store for multipart uploads
# Use --workers N if multipart state is moved to Redis/distributed store
CMD ["uvicorn", "s3proxy.main:app", "--host", "0.0.0.0", "--port", "4433", "--loop", "uvloop"]
