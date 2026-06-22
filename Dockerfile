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

EXPOSE 8080

# app.main starts a stdlib HTTP health server (:8080/healthz, /ready) on a
# daemon thread before the Celery worker boots, used for HEALTHCHECK here
# and for the K8s liveness/readiness probes in the Helm chart.
HEALTHCHECK --interval=60s --timeout=15s --start-period=30s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8080/healthz')"

CMD ["celery", "-A", "app.main.app", "worker", "--loglevel=info", "-Q", "remediation", "-c", "2"]
