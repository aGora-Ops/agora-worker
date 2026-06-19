from app.core.config import settings

broker_url = settings.REDIS_URL
result_backend = settings.REDIS_URL

task_serializer = "json"
accept_content = ["json"]
result_serializer = "json"

timezone = "UTC"
enable_utc = True

task_routes = {
    "app.tasks.remediation.process_failed_workflow": {"queue": "remediation"},
    "app.tasks.remediation.upsert_workflow_run_task": {"queue": "remediation"},
    "app.tasks.remediation.backfill_org_runs_task": {"queue": "remediation"},
}

task_acks_late = True
worker_prefetch_multiplier = 1

result_expires = 86400
