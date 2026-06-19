"""Celery tasks for the remediation pipeline.

Architecture (suggestion-engine model):
  1. Worker is READ-ONLY with respect to GitHub — no branch creation, no
     commits, no PRs. Write-scope GitHub operations are user-triggered via
     the api-service POST /api/v1/remediations/{id}/raise-pr endpoint.
  2. This eliminates the prompt-injection blast radius: even if Bedrock is
     manipulated via log content, the worst outcome is a bad YAML suggestion
     the user can reject — not an auto-committed backdoor.
  3. After analysis, status becomes 'analyzed' and suggested_yaml is populated.
     The user reviews in the dashboard and clicks "Raise PR" to create the PR.
"""
import json
import logging
import time
import uuid
from datetime import datetime, timezone

from cryptography.fernet import InvalidToken
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from app.core.celery_app import app
from app.core.config import settings
from app.core.security import decrypt_token
from app.services.bedrock_client import BedrockRemediationClient
from app.services.github_client import GitHubRemediationClient

logger = logging.getLogger(__name__)

_sync_engine = create_engine(
    settings.DATABASE_URL,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
)
SyncSessionLocal = sessionmaker(bind=_sync_engine, autocommit=False, autoflush=False)

REDIS_EVENTS_CHANNEL = "agora:events"

def _publish_event(event_type: str, data: dict) -> None:
    """Fire-and-forget Redis publish so WebSocket clients get live updates."""
    try:
        import redis as redis_sync
        r = redis_sync.Redis.from_url(settings.REDIS_URL)
        r.publish(REDIS_EVENTS_CHANNEL, json.dumps({"type": event_type, "data": data}))
        r.close()
    except Exception as exc:
        logger.warning("Failed to publish %s event to Redis: %s", event_type, exc)

def _get_github_token_for_org(session: Session, org_login: str) -> str:
    row = session.execute(
        text(
            """
            SELECT u.access_token_encrypted
            FROM organizations o
            JOIN users u ON u.id = o.owner_id
            WHERE o.login = :login
            LIMIT 1
            """
        ),
        {"login": org_login},
    ).fetchone()

    if row:
        try:
            return decrypt_token(row[0])
        except InvalidToken as exc:
            raise RuntimeError(
                f"Token decryption failed for org '{org_login}'. "
                "Ensure SECRET_KEY / TOKEN_ENCRYPTION_KEY is identical across "
                "api-service and remediation-worker."
            ) from exc

    if settings.GITHUB_TOKEN:
        return settings.GITHUB_TOKEN

    raise RuntimeError(f"No GitHub token available for org '{org_login}'")

def _parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None

def _upsert_workflow_run(session: Session, message: dict) -> uuid.UUID:
    """Insert or update a WorkflowRun row, returning its UUID."""
    run_id = message["run_id"]
    status_value = message.get("status") or "queued"
    conclusion = message.get("conclusion")
    started_at = _parse_timestamp(message.get("started_at"))
    completed_at = _parse_timestamp(message.get("completed_at"))
    html_url = message.get("html_url") or (
        f"https://github.com/{message['repo_owner']}/{message['repo_name']}"
        f"/actions/runs/{run_id}"
    )
    now = datetime.now(timezone.utc)

    row = session.execute(
        text(
            """
            INSERT INTO workflow_runs (
                id, github_run_id, github_workflow_id, org_login, repo_name,
                workflow_name, workflow_file, branch, head_sha,
                status, conclusion, started_at, completed_at, html_url,
                created_at, updated_at
            ) VALUES (
                :id, :run_id, :workflow_id, :org_login, :repo_name,
                :workflow_name, :workflow_file, :branch, :head_sha,
                :status, :conclusion, :started_at, :completed_at, :html_url,
                :created_at, :updated_at
            )
            ON CONFLICT (github_run_id) DO UPDATE SET
                status = EXCLUDED.status,
                conclusion = EXCLUDED.conclusion,
                started_at = COALESCE(EXCLUDED.started_at, workflow_runs.started_at),
                completed_at = COALESCE(EXCLUDED.completed_at, workflow_runs.completed_at),
                html_url = EXCLUDED.html_url,
                updated_at = EXCLUDED.updated_at
            RETURNING id
            """
        ),
        {
            "id": str(uuid.uuid4()),
            "run_id": run_id,
            "workflow_id": message.get("workflow_id", 0),
            "org_login": message["repo_owner"],
            "repo_name": message["repo_name"],
            "workflow_name": message.get("workflow_name", ""),
            "workflow_file": message.get("workflow_file", ""),
            "branch": message.get("branch", ""),
            "head_sha": message.get("head_sha", ""),
            "status": status_value,
            "conclusion": conclusion,
            "started_at": started_at,
            "completed_at": completed_at,
            "html_url": html_url,
            "created_at": now,
            "updated_at": now,
        },
    ).fetchone()
    session.commit()
    return uuid.UUID(str(row[0]))

def _create_remediation_record(
    session: Session,
    workflow_run_id: uuid.UUID,
    message: dict,
    status: str,
) -> uuid.UUID:
    """Insert a Remediation row with the given status and return its UUID."""
    remediation_id = uuid.uuid4()
    now = datetime.now(timezone.utc)
    session.execute(
        text(
            """
            INSERT INTO remediations (
                id, workflow_run_id, org_login, repo_name, workflow_file,
                root_cause, fixed_yaml, suggested_yaml,
                bedrock_model, status, created_at, updated_at
            ) VALUES (
                :id, :workflow_run_id, :org_login, :repo_name, :workflow_file,
                '', '', NULL,
                :bedrock_model, :status, :created_at, :updated_at
            )
            """
        ),
        {
            "id": str(remediation_id),
            "workflow_run_id": str(workflow_run_id),
            "org_login": message["repo_owner"],
            "repo_name": message["repo_name"],
            "workflow_file": message.get("workflow_file", ""),
            "bedrock_model": settings.BEDROCK_MODEL_ID,
            "status": status,
            "created_at": now,
            "updated_at": now,
        },
    )
    session.commit()
    return remediation_id

def _update_remediation(
    session: Session,
    remediation_id: uuid.UUID,
    status: str,
    root_cause: str = "",
    suggested_yaml: str | None = None,
    error_message: str | None = None,
) -> None:
    """Update an existing Remediation row."""
    session.execute(
        text(
            """
            UPDATE remediations SET
                status = :status,
                root_cause = :root_cause,
                suggested_yaml = :suggested_yaml,
                error_message = :error_message,
                updated_at = :updated_at
            WHERE id = :id
            """
        ),
        {
            "id": str(remediation_id),
            "status": status,
            "root_cause": root_cause,
            "suggested_yaml": suggested_yaml,
            "error_message": error_message,
            "updated_at": datetime.now(timezone.utc),
        },
    )
    session.commit()

@app.task(bind=True, max_retries=3, default_retry_delay=30)
def upsert_workflow_run_task(self, message: dict) -> dict:
    """Persist/update a workflow_run row for non-failure events."""
    session = SyncSessionLocal()
    try:
        run_uuid = _upsert_workflow_run(session, message)
        _publish_event("run_update", {
            "run_id": str(run_uuid),
            "github_run_id": message["run_id"],
            "status": message.get("status"),
            "conclusion": message.get("conclusion"),
            "org_login": message["repo_owner"],
            "repo_name": message["repo_name"],
        })
        return {"status": "synced", "workflow_run_id": str(run_uuid)}
    except Exception as exc:
        logger.exception("Failed to sync workflow run %s: %s", message.get("run_id"), exc)
        raise self.retry(exc=exc)
    finally:
        session.close()

@app.task(bind=True, max_retries=3, default_retry_delay=60)
def process_failed_workflow(self, message: dict) -> dict:
    """
    Analyze a failed workflow run with Bedrock and store a suggested fix.

    This task is READ-ONLY with respect to GitHub. It fetches the workflow
    YAML and logs, sends them to Bedrock for analysis, then stores the
    suggested_yaml and root_cause in the remediation record. No branches,
    commits, or PRs are created — those happen only when the user clicks
    "Raise PR" in the dashboard (POST /api/v1/remediations/{id}/raise-pr).
    """
    repo_owner: str = message["repo_owner"]
    repo_name: str = message["repo_name"]
    run_id: int = message["run_id"]
    workflow_file: str = message.get("workflow_file", "")
    head_sha: str = message.get("head_sha", "")
    workflow_name: str = message.get("workflow_name", "")

    logger.info("Analyzing failed workflow run %s for %s/%s", run_id, repo_owner, repo_name)

    session = SyncSessionLocal()
    github: GitHubRemediationClient | None = None
    workflow_run_id: uuid.UUID | None = None
    remediation_id: uuid.UUID | None = None

    try:
        workflow_run_id = _upsert_workflow_run(session, message)
        _publish_event("run_update", {
            "run_id": str(workflow_run_id),
            "status": "completed",
            "conclusion": "failure",
            "org_login": repo_owner,
            "repo_name": repo_name,
        })

        remediation_id = _create_remediation_record(session, workflow_run_id, message, "analyzing")
        _publish_event("remediation_created", {"id": str(remediation_id), "status": "analyzing"})

        github_token = _get_github_token_for_org(session, repo_owner)
        github = GitHubRemediationClient(github_token)

        logger.info("Fetching workflow YAML: %s@%s", workflow_file, head_sha)
        workflow_yaml = github.get_workflow_yaml(repo_owner, repo_name, workflow_file, head_sha)

        logger.info("Fetching logs for run %s", run_id)
        logs = github.get_run_logs(repo_owner, repo_name, run_id)

        from app.agents.scrubber import scrub
        scrubbed_logs = scrub(logs)

        if settings.USE_MULTI_AGENT:
            logger.info("Running multi-agent LangGraph pipeline for run %s", run_id)
            from app.agents.graph import remediation_graph
            final_state = remediation_graph.invoke({
                "repo_owner": repo_owner,
                "repo_name": repo_name,
                "workflow_file": workflow_file,
                "workflow_yaml": workflow_yaml,
                "logs": scrubbed_logs,
                "agent_trace": [],
            })
            if final_state.get("error"):
                raise RuntimeError(final_state["error"])
            root_cause = final_state.get("root_cause", "")
            suggested_yaml = final_state.get("suggested_yaml")
            pr_title = final_state.get("pr_title", "")
            logger.info("Multi-agent trace: %s", final_state.get("agent_trace", []))
        else:
            logger.info("Invoking Bedrock (single-agent) for run %s", run_id)
            bedrock = BedrockRemediationClient()
            analysis = bedrock.analyze_failure(
                workflow_yaml, scrubbed_logs, workflow_name, f"{repo_owner}/{repo_name}"
            )
            root_cause = analysis.get("root_cause", "")
            suggested_yaml = analysis.get("fixed_yaml")
            pr_title = analysis.get("pr_title", "")

        _update_remediation(
            session,
            remediation_id,
            status="analyzed",
            root_cause=root_cause,
            suggested_yaml=suggested_yaml,
        )
        _publish_event("remediation_updated", {
            "id": str(remediation_id),
            "status": "analyzed",
            "root_cause": root_cause,
        })

        logger.info("Analysis completed for run %s (remediation %s)", run_id, remediation_id)
        return {"status": "analyzed", "remediation_id": str(remediation_id)}

    except Exception as exc:
        logger.exception("Analysis failed for run %s: %s", run_id, exc)

        if remediation_id:
            try:
                _update_remediation(
                    session, remediation_id, status="failed", error_message=str(exc)
                )
                _publish_event("remediation_updated", {
                    "id": str(remediation_id),
                    "status": "failed",
                })
            except Exception as update_exc:
                logger.error("Failed to mark remediation as failed: %s", update_exc)

        raise self.retry(exc=exc)

    finally:
        session.close()
        if github:
            github.close()

_BACKFILL_MAX_RUNS_PER_REPO = 200
_BACKFILL_LOW_RATE_LIMIT_THRESHOLD = 50

def _set_org_sync_status(session: Session, org_login: str, status_value: str) -> None:
    session.execute(
        text("UPDATE organizations SET sync_status = :status WHERE login = :login"),
        {"status": status_value, "login": org_login},
    )
    session.commit()

@app.task(bind=True, max_retries=2, default_retry_delay=120)
def backfill_org_runs_task(self, org_login: str) -> dict:
    """One-shot historical sync for a newly connected org."""
    session = SyncSessionLocal()
    github: GitHubRemediationClient | None = None
    synced = 0

    try:
        _set_org_sync_status(session, org_login, "syncing")
        github_token = _get_github_token_for_org(session, org_login)
        github = GitHubRemediationClient(github_token)

        repos = github.get_org_repos(org_login)
        for repo in repos:
            repo_name = repo["name"]
            page = 1
            repo_synced = 0

            while repo_synced < _BACKFILL_MAX_RUNS_PER_REPO:
                runs, rate_limit_remaining = github.get_repo_runs(
                    org_login, repo_name, per_page=100, page=page
                )
                if not runs:
                    break

                for run in runs:
                    msg = {
                        "repo_owner": org_login,
                        "repo_name": repo_name,
                        "run_id": run["id"],
                        "workflow_id": run.get("workflow_id"),
                        "workflow_name": run.get("name", ""),
                        "workflow_file": run.get("path", ""),
                        "branch": run.get("head_branch", ""),
                        "head_sha": run.get("head_sha", ""),
                        "status": run.get("status"),
                        "conclusion": run.get("conclusion"),
                        "started_at": run.get("run_started_at"),
                        "completed_at": run.get("updated_at")
                        if run.get("status") == "completed"
                        else None,
                        "html_url": run.get("html_url"),
                    }
                    _upsert_workflow_run(session, msg)
                    repo_synced += 1
                    synced += 1
                    if repo_synced >= _BACKFILL_MAX_RUNS_PER_REPO:
                        break

                if len(runs) < 100:
                    break
                page += 1

                if (
                    rate_limit_remaining is not None
                    and rate_limit_remaining < _BACKFILL_LOW_RATE_LIMIT_THRESHOLD
                ):
                    logger.warning("Rate limit low (%s), pausing backfill", rate_limit_remaining)
                    time.sleep(5)

            time.sleep(0.5)

        _set_org_sync_status(session, org_login, "completed")
        logger.info("Backfill completed for %s: %s runs", org_login, synced)
        return {"status": "completed", "org_login": org_login, "synced": synced}

    except Exception as exc:
        logger.exception("Backfill failed for %s: %s", org_login, exc)
        try:
            _set_org_sync_status(session, org_login, "failed")
        except Exception:
            pass
        raise self.retry(exc=exc)

    finally:
        session.close()
        if github:
            github.close()
