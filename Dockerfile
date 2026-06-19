# ---- Stage 1: builder ----
FROM python:3.12-slim AS builder

WORKDIR /build

RUN pip install --upgrade pip

COPY pyproject.toml .
RUN pip install --prefix=/install .

# ---- Stage 2: runtime ----
FROM python:3.12-slim

WORKDIR /app

RUN addgroup --system --gid 1001 appgroup \
 && adduser --system --uid 1001 --ingroup appgroup appuser

COPY --from=builder /install /usr/local
COPY app/ ./app/

RUN chown -R appuser:appgroup /app
USER appuser

# Celery doesn't expose an HTTP port, but we can verify the worker is alive
# by checking that the process started. The liveness probe in Kubernetes uses
# "celery inspect ping" instead; this HEALTHCHECK is for docker-compose use.
HEALTHCHECK --interval=60s --timeout=15s --start-period=30s --retries=3 \
  CMD celery -A app.main.app inspect ping -d "celery@$HOSTNAME" || exit 1

CMD ["celery", "-A", "app.main.app", "worker", "--loglevel=info", "-Q", "remediation", "-c", "2"]
