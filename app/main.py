
from app.core.celery_app import app
from app.core.health import mark_ready, start_health_server
from app.tasks import remediation

start_health_server(port=8080)
mark_ready()
