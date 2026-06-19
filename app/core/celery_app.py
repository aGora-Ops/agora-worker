from celery import Celery

app = Celery("remediation-worker")
app.config_from_object("app.core.celery_config")
