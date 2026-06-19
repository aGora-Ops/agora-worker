"""
SQS -> Celery bridge.

webhook-service and api-service publish raw JSON messages directly to SQS via
boto3 (see their SQSPublisher classes) — they do not speak Celery's wire
protocol. Celery's broker here is Redis (see core/celery_config.py), not SQS.
This process is the missing link: it long-polls the SQS queue, parses each
message, and dispatches it to the appropriate Celery task via .delay(), which
enqueues onto Redis for the `celery worker` process to pick up.

Runs as its own process/container — separate from the Celery worker — so it
can scale independently (it's I/O-bound polling, not CPU/LLM-bound work).
"""
import json
import logging
import time

import boto3

from app.core.config import settings
from app.tasks.remediation import (
    backfill_org_runs_task,
    process_failed_workflow,
    upsert_workflow_run_task,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_POLL_WAIT_SECONDS = 20
_VISIBILITY_TIMEOUT = 60

def _dispatch(message: dict) -> None:
    event_type = message.get("event_type")

    if event_type == "workflow_run":
        if message.get("requires_analysis"):
            process_failed_workflow.delay(message)
        else:
            upsert_workflow_run_task.delay(message)
        return

    if event_type == "backfill_org":
        backfill_org_runs_task.delay(message["org_login"])
        return

    logger.warning("Unknown event_type in SQS message, dropping: %r", event_type)

def run() -> None:
    """Long-poll the SQS queue forever, dispatching each message to Celery."""
    client = boto3.client("sqs", region_name=settings.AWS_REGION)
    queue_url = settings.SQS_QUEUE_URL
    logger.info("Starting SQS consumer, polling %s", queue_url)

    while True:
        try:
            response = client.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=_POLL_WAIT_SECONDS,
                VisibilityTimeout=_VISIBILITY_TIMEOUT,
            )
        except Exception as exc:
            logger.warning("SQS receive_message failed (no AWS creds?): %s", exc)
            time.sleep(30)
            continue

        for sqs_message in response.get("Messages", []):
            receipt_handle = sqs_message["ReceiptHandle"]
            try:
                body = json.loads(sqs_message["Body"])
                _dispatch(body)
            except Exception as exc:
                logger.exception("Failed to process SQS message, dropping: %s", exc)
            finally:
                client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)

if __name__ == "__main__":
    run()
