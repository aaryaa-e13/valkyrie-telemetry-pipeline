# VALKYRIE simulator image.
# Build context: repository root (see docker-compose.yml).
FROM python:3.11.18-slim-bookworm

# Unbuffered output so logs appear immediately; no .pyc files in the image.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Install dependencies first so this layer is cached when only code changes.
COPY requirements/simulator.txt requirements/simulator.txt
RUN pip install -r requirements/simulator.txt

COPY common/ common/
COPY simulator/ simulator/

# Run as an unprivileged user (uid 1000 matches the usual WSL user, so files
# written to the mounted manifests/ directory are owned by you, not root).
RUN useradd --uid 1000 --create-home appuser && mkdir -p /app/manifests \
    && chown appuser /app/manifests
USER appuser

ENTRYPOINT ["python", "-m", "simulator.main"]
CMD ["--config", "simulator/config.yaml"]
