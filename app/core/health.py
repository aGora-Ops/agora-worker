"""Minimal stdlib HTTP server exposing /healthz and /ready.

Celery and the SQS consumer have no web framework and no HTTP port. K8s
liveness/readiness probes need an HTTP endpoint, so this runs a tiny
http.server on a daemon thread alongside the real process — no new
dependency, no interference with Celery's own event loop / the consumer's
polling loop.
"""
import logging
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

logger = logging.getLogger(__name__)

_ready = threading.Event()


def mark_ready() -> None:
    """Call once the process has finished startup and is doing real work."""
    _ready.set()


class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path == "/healthz":
            self._respond(200, b"ok")
        elif self.path == "/ready":
            if _ready.is_set():
                self._respond(200, b"ok")
            else:
                self._respond(503, b"not ready")
        else:
            self._respond(404, b"not found")

    def _respond(self, status: int, body: bytes) -> None:
        self.send_response(status)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args) -> None:
        pass


def start_health_server(port: int = 8080) -> ThreadingHTTPServer:
    """Start the health server on a daemon thread and return it."""
    server = ThreadingHTTPServer(("0.0.0.0", port), _HealthHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    logger.info("Health server listening on :%d (/healthz, /ready)", port)
    return server
