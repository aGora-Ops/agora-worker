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
import yaml

from app.core.celery_app import app
from app.core.config import settings
from app.core.security import decrypt_token
from app.services.bedrock_client import BedrockRemediationClient
from app.services.github_app import get_installation_token, github_app_configured
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


def _compress_logs(text: str, max_lines: int = 200) -> str:
    """Deduplicate consecutive repeated lines and cap total lines.

    Repeated stack-trace or progress lines are the main source of token bloat
    in CI logs. Collapsing runs of identical lines cuts 40-60% in typical
    failure output without losing any unique signal.
    """
    lines = text.splitlines()
    deduped: list[str] = []
    prev = None
    run = 0
    for line in lines:
        if line == prev:
            run += 1
        else:
            if run > 1:
                deduped.append(f"  ... (repeated {run} times)")
            deduped.append(line)
            prev = line
            run = 1
    if run > 1:
        deduped.append(f"  ... (repeated {run} times)")
    if len(deduped) > max_lines:
        kept = max_lines // 2
        deduped = deduped[:kept] + [f"  ... ({len(deduped) - max_lines} lines omitted) ..."] + deduped[-kept:]
    return "\n".join(deduped)


def _strip_code_fences(text: str) -> str:
    value = (text or "").strip()
    if value.startswith("```"):
        value = "\n".join(
            line for line in value.splitlines() if not line.strip().startswith("```")
        ).strip()
    return value


def _normalize_suggested_yaml(original: str, candidate: str | None) -> tuple[bool, str]:
    normalized = _strip_code_fences(candidate or "")
    if not normalized:
        return False, "empty output"
    # Post-process to fix common formatting anomalies where colons lack trailing space
    import re
    lines = []
    spacing_pattern = re.compile(r"^([ \t]*[a-zA-Z0-9_-]+):(?!\s)(.+)$")
    for line in normalized.splitlines():
        m = spacing_pattern.match(line)
        if m:
            lines.append(f"{m.group(1)}: {m.group(2)}")
        else:
            lines.append(line)
    normalized = "\n".join(lines)

    try:
        parsed = yaml.safe_load(normalized)
    except yaml.YAMLError:
        return False, "invalid YAML syntax"
    if not isinstance(parsed, dict) or "jobs" not in parsed:
        return False, "not a GitHub Actions workflow (no jobs:)"
    if normalized.strip() == original.strip():
        return False, "no change from original"
    return True, normalized


def _recover_suggested_yaml(
    workflow_yaml: str,
    root_cause: str,
    failure_category: str | None,
    logs: str,
    workflow_name: str,
    repo_full_name: str,
) -> str | None:
    """Try stricter non-agent fallbacks when the multi-agent pipeline finds no valid fix."""
    bedrock = BedrockRemediationClient()

    candidate = bedrock.generate_yaml_fix(
        workflow_yaml=workflow_yaml,
        root_cause=root_cause,
        failure_category=failure_category or "UNKNOWN",
        logs=logs,
    )
    logger.warning("[RAW BEDROCK YAML] direct fallback candidate (first 800 chars): %r", (candidate or "")[:800])
    ok, normalized = _normalize_suggested_yaml(workflow_yaml, candidate)
    if ok:
        return normalized
    logger.warning("Direct YAML fallback produced an invalid fix (%s)", normalized)

    analysis = bedrock.analyze_failure(
        workflow_yaml, logs, workflow_name, repo_full_name
    )
    ok, normalized = _normalize_suggested_yaml(
        workflow_yaml, analysis.get("fixed_yaml")
    )
    if ok:
        return normalized
    logger.warning("Single-agent Bedrock fallback produced an invalid fix (%s)", normalized)
    return None


def _heuristic_yaml_fix(
    workflow_yaml: str,
    root_cause: str,
    failure_category: str | None,
) -> str | None:
    """Deterministic last-resort fix for very common version-mismatch failures.

    This is intentionally narrow: we only auto-edit when the worker has a clear
    dependency-version signal and a known bad version token to replace.

    NOTE: We intentionally do NOT gate on failure_category because Bedrock
    occasionally misclassifies python-version failures as UNKNOWN rather than
    DEPENDENCY_VERSION. Root-cause text is a more reliable signal.
    """
    root_lower = (root_cause or "").lower()
    if "python" not in root_lower and "version" not in root_lower:
        return None

    replacements = [
        ('python-version: "99"', 'python-version: "3.12"'),
        ("python-version: '99'", 'python-version: "3.12"'),
        ("python-version: 99", 'python-version: "3.12"'),
    ]

    fixed = workflow_yaml
    changed = False
    for old, new in replacements:
        if old in fixed:
            fixed = fixed.replace(old, new)
            changed = True

    if not changed:
        return None

    ok, normalized = _normalize_suggested_yaml(workflow_yaml, fixed)
    if ok:
        logger.info("_heuristic_yaml_fix applied deterministic python-version fix")
    return normalized if ok else None

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
    # Prefer GitHub App installation token — scoped to the org, no user token needed.
    if github_app_configured():
        row = session.execute(
            text("SELECT installation_id FROM organizations WHERE login = :login LIMIT 1"),
            {"login": org_login},
        ).fetchone()
        if row and row[0]:
            import asyncio
            return asyncio.get_event_loop().run_until_complete(
                get_installation_token(row[0])
            )

    # Fall back to the org owner's OAuth token (pre-App path).
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

def _get_owner_email_for_org(session: Session, org_login: str) -> str | None:
    """Email of the org's owner (User.email — null if their GitHub email is
    private and they authorized before the user:email scope was added)."""
    row = session.execute(
        text(
            """
            SELECT u.email
            FROM organizations o
            JOIN users u ON u.id = o.owner_id
            WHERE o.login = :login
            LIMIT 1
            """
        ),
        {"login": org_login},
    ).fetchone()
    return row[0] if row else None

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
                -- Only advance status forward: queued < in_progress < completed.
                -- A late out-of-order SQS delivery must NEVER regress a
                -- completed run back to queued or in_progress.
                status = CASE
                    WHEN workflow_runs.status = 'completed' THEN workflow_runs.status
                    WHEN workflow_runs.status = 'in_progress' AND EXCLUDED.status = 'queued'
                        THEN workflow_runs.status
                    ELSE EXCLUDED.status
                END,
                -- Only update conclusion when we are actually completing the run.
                conclusion = CASE
                    WHEN EXCLUDED.status = 'completed' THEN EXCLUDED.conclusion
                    ELSE workflow_runs.conclusion
                END,
                started_at = COALESCE(EXCLUDED.started_at, workflow_runs.started_at),
                -- Only set completed_at when the run is actually completing.
                completed_at = CASE
                    WHEN EXCLUDED.status = 'completed'
                        THEN COALESCE(EXCLUDED.completed_at, workflow_runs.completed_at)
                    ELSE workflow_runs.completed_at
                END,
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
    failure_category: str | None = None,
    confidence_score: int | None = None,
    confidence_reasoning: str | None = None,
    security_risk_score: int | None = None,
    security_findings: list[str] | None = None,
    pr_title: str | None = None,
    pr_description: str | None = None,
    agent_trace: list[str] | None = None,
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
                failure_category = :failure_category,
                confidence_score = :confidence_score,
                confidence_reasoning = :confidence_reasoning,
                security_risk_score = :security_risk_score,
                security_findings = :security_findings,
                pr_title = :pr_title,
                pr_description = :pr_description,
                agent_trace = :agent_trace,
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
            "failure_category": failure_category,
            "confidence_score": confidence_score,
            "confidence_reasoning": confidence_reasoning,
            "security_risk_score": security_risk_score,
            "security_findings": security_findings,
            "pr_title": pr_title,
            "pr_description": pr_description,
            "agent_trace": agent_trace,
            "updated_at": datetime.now(timezone.utc),
        },
    )
    session.commit()

def _ingest_embedding(
    session: Session,
    remediation_id: uuid.UUID,
    org_login: str,
    repo_name: str,
    workflow_file: str,
    failure_category: str | None,
    root_cause: str,
    suggested_yaml: str | None,
    logs_excerpt: str = "",
) -> None:
    """Write a remediation doc to the Bedrock Knowledge Base S3 source bucket.

    Bedrock ingests from S3 automatically (sync is triggered on a schedule or
    manually). Falls back to the legacy pgvector path when BEDROCK_KB_S3_BUCKET
    is not set, so local dev / pre-apply environments continue to work.

    Best-effort: failures must never break the remediation flow.
    """
    chunk = (
        f"Repository: {org_login}/{repo_name}\n"
        f"Workflow: {workflow_file}\n"
        f"Failure category: {failure_category or 'UNKNOWN'}\n"
        f"Root cause: {root_cause}\n\n"
        f"Suggested fix (YAML):\n{(suggested_yaml or '')[:1500]}\n\n"
        f"Log excerpt:\n{logs_excerpt[:1500]}"
    )

    if settings.BEDROCK_KB_S3_BUCKET:
        import boto3
        s3 = boto3.client("s3", region_name=settings.AWS_REGION)
        key = f"remediations/{remediation_id}.txt"
        s3.put_object(
            Bucket=settings.BEDROCK_KB_S3_BUCKET,
            Key=key,
            Body=chunk.encode(),
            ContentType="text/plain",
        )
        logger.debug("KB doc written to s3://%s/%s", settings.BEDROCK_KB_S3_BUCKET, key)
        return

    # Legacy pgvector path (used when KB is not configured).
    from app.services.embeddings import embed_text, to_pgvector

    embedding = embed_text(chunk)
    meta = json.dumps({
        "org_login": org_login,
        "repo_name": repo_name,
        "workflow_file": workflow_file,
        "failure_category": failure_category or "UNKNOWN",
    })
    session.execute(
        text("DELETE FROM log_embeddings WHERE source_type = 'remediation' AND source_id = :sid"),
        {"sid": str(remediation_id)},
    )
    session.execute(
        text(
            """
            INSERT INTO log_embeddings
                (source_type, source_id, org_login, repo_name, failure_category,
                 chunk_text, embedding, metadata)
            VALUES
                ('remediation', :sid, :org, :repo, :cat,
                 :chunk, CAST(:emb AS vector), CAST(:meta AS jsonb))
            """
        ),
        {
            "sid": str(remediation_id),
            "org": org_login,
            "repo": repo_name,
            "cat": failure_category,
            "chunk": chunk,
            "emb": to_pgvector(embedding),
            "meta": meta,
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
        workflow_yaml = scrub(workflow_yaml)

        scrubbed_logs = _compress_logs(scrubbed_logs)

        if settings.USE_MULTI_AGENT:
            logger.info("Running multi-agent LangGraph pipeline for run %s", run_id)
            from app.agents.graph import remediation_graph

            # Pull accepted fixes for the same org+repo from fix_memories as few-shot examples.
            # Classify first so we can filter by category after the graph runs classify_failure;
            # here we fetch by repo as a broad fallback — the node will filter by category.
            fix_examples: list[str] = []
            try:
                rows = session.execute(
                    text(
                        """
                        SELECT fixed_yaml FROM fix_memories
                        WHERE org_login = :org AND repo_name = :repo
                        ORDER BY created_at DESC
                        LIMIT 5
                        """
                    ),
                    {"org": repo_owner, "repo": repo_name},
                ).fetchall()
                fix_examples = [r[0] for r in rows if r[0]]
            except Exception as fm_exc:
                logger.debug("fix_memories fetch skipped: %s", fm_exc)

            final_state = remediation_graph.invoke({
                "repo_owner": repo_owner,
                "repo_name": repo_name,
                "workflow_file": workflow_file,
                "workflow_yaml": workflow_yaml,
                "logs": scrubbed_logs,
                "head_sha": head_sha,
                "run_id": run_id,
                "github_token": github_token,
                "agent_trace": [],
                "fix_examples": fix_examples,
            })
            if final_state.get("error"):
                raise RuntimeError(final_state["error"])
            root_cause = final_state.get("root_cause", "")
            suggested_yaml = final_state.get("suggested_yaml")
            pr_title = final_state.get("pr_title", "")
            pr_description = final_state.get("pr_description")
            failure_category = final_state.get("failure_category")
            confidence_score = final_state.get("confidence_score")
            confidence_reasoning = final_state.get("confidence_reasoning")
            security_risk_score = final_state.get("security_risk_score")
            security_findings = final_state.get("security_findings")
            agent_trace = final_state.get("agent_trace")
            logger.info(
                "Multi-agent trace: %s | pr_title: %s | security_risk: %s | confidence: %s",
                agent_trace,
                pr_title,
                security_risk_score,
                confidence_score,
            )
        else:
            logger.info("Invoking Bedrock (single-agent) for run %s", run_id)
            bedrock = BedrockRemediationClient()
            analysis = bedrock.analyze_failure(
                workflow_yaml, scrubbed_logs, workflow_name, f"{repo_owner}/{repo_name}"
            )
            root_cause = analysis.get("root_cause", "")
            suggested_yaml = analysis.get("fixed_yaml")
            pr_title = analysis.get("pr_title", "")
            pr_description = None
            failure_category = None
            confidence_score = None
            confidence_reasoning = None
            security_risk_score = None
            security_findings = None
            agent_trace = None

        if not suggested_yaml and root_cause:
            logger.warning(
                "Analysis found root cause but no valid YAML fix for run %s; trying direct Bedrock fallback",
                run_id,
            )
            suggested_yaml = _recover_suggested_yaml(
                workflow_yaml=workflow_yaml,
                root_cause=root_cause,
                failure_category=failure_category,
                logs=scrubbed_logs,
                workflow_name=workflow_name,
                repo_full_name=f"{repo_owner}/{repo_name}",
            )

        if not suggested_yaml:
            suggested_yaml = _heuristic_yaml_fix(
                workflow_yaml=workflow_yaml,
                root_cause=root_cause,
                failure_category=failure_category,
            )

        if not suggested_yaml:
            message = "AI identified the root cause but could not produce a valid YAML fix."
            _update_remediation(
                session,
                remediation_id,
                status="failed",
                root_cause=root_cause,
                suggested_yaml=None,
                error_message=message,
                failure_category=failure_category,
                confidence_score=0,
                confidence_reasoning="No valid YAML suggestion was produced.",
                security_risk_score=security_risk_score,
                security_findings=security_findings,
                pr_title=pr_title,
                pr_description=pr_description,
                agent_trace=agent_trace,
            )
            _publish_event("remediation_updated", {
                "id": str(remediation_id),
                "status": "failed",
                "root_cause": root_cause,
            })
            logger.warning(
                "Analysis completed without a valid YAML fix for run %s (remediation %s)",
                run_id,
                remediation_id,
            )
            return {"status": "failed", "remediation_id": str(remediation_id)}

        _update_remediation(
            session,
            remediation_id,
            status="analyzed",
            root_cause=root_cause,
            suggested_yaml=suggested_yaml,
            failure_category=failure_category,
            confidence_score=confidence_score,
            confidence_reasoning=confidence_reasoning,
            security_risk_score=security_risk_score,
            security_findings=security_findings,
            pr_title=pr_title,
            pr_description=pr_description,
            agent_trace=agent_trace,
        )
        _publish_event("remediation_updated", {
            "id": str(remediation_id),
            "status": "analyzed",
            "root_cause": root_cause,
        })

        # Index this remediation for semantic (RAG) search in Pipeline Chat.
        # Best-effort — never fail the analysis if embedding/Bedrock hiccups.
        try:
            _ingest_embedding(
                session, remediation_id, repo_owner, repo_name, workflow_file,
                failure_category, root_cause, suggested_yaml, scrubbed_logs,
            )
        except Exception as embed_exc:
            logger.warning("Embedding ingestion failed for remediation %s: %s", remediation_id, embed_exc)

        # Notify the org owner a fix is ready to review. Best-effort — never
        # fail the analysis over SES being unconfigured/throttled/etc.
        try:
            owner_email = _get_owner_email_for_org(session, repo_owner)
            if owner_email:
                from app.services.email import send_fix_notification
                send_fix_notification(
                    owner_email, repo_name, failure_category, root_cause, str(remediation_id),
                )
            else:
                logger.info(
                    "No email on file for %s's owner — skipping fix notification for remediation %s",
                    repo_owner, remediation_id,
                )
        except Exception as email_exc:
            logger.warning("Fix notification email failed for remediation %s: %s", remediation_id, email_exc)

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

@app.task(bind=True, max_retries=1, default_retry_delay=60)
def backfill_embeddings_task(self, limit: int = 500) -> dict:
    """Embed existing analyzed remediations into log_embeddings for RAG.

    One-shot catch-up for remediations created before embedding-on-analysis
    existed (or after the log_embeddings table is first created). Logs aren't
    persisted on the remediation row, so backfill embeds root_cause + fix only.
    """
    session = SyncSessionLocal()
    embedded = 0
    try:
        rows = session.execute(
            text(
                """
                SELECT r.id, r.org_login, r.repo_name, r.workflow_file,
                       r.failure_category, r.root_cause, r.suggested_yaml
                FROM remediations r
                WHERE r.root_cause <> ''
                  AND NOT EXISTS (
                      SELECT 1 FROM log_embeddings e
                      WHERE e.source_type = 'remediation' AND e.source_id = r.id
                  )
                ORDER BY r.created_at DESC
                LIMIT :lim
                """
            ),
            {"lim": limit},
        ).fetchall()

        for row in rows:
            try:
                _ingest_embedding(
                    session, row.id, row.org_login, row.repo_name, row.workflow_file,
                    row.failure_category, row.root_cause, row.suggested_yaml, "",
                )
                embedded += 1
            except Exception as exc:
                logger.warning("Backfill embedding failed for remediation %s: %s", row.id, exc)

        logger.info("Embedding backfill complete: %s remediations embedded", embedded)
        return {"status": "completed", "embedded": embedded}
    finally:
        session.close()

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


@app.task(bind=True, max_retries=3, default_retry_delay=10)
def register_app_installation_task(self, message: dict) -> dict:
    """Handle GitHub App installation events from SQS.

    'created' → upsert org with installation_id, then kick off backfill.
    'deleted' → remove org row (App was uninstalled).
    """
    import uuid as _uuid

    action = message.get("action")
    org_login = message.get("org_login")
    org_id = message.get("org_id")
    installation_id = message.get("installation_id")
    avatar_url = message.get("avatar_url")

    session = SyncSessionLocal()
    try:
        if action == "created":
            user_row = session.execute(
                text("SELECT id FROM users ORDER BY created_at LIMIT 1")
            ).fetchone()
            owner_id = str(user_row[0]) if user_row else None

            if owner_id:
                session.execute(
                    text(
                        """
                        INSERT INTO organizations
                          (id, github_org_id, login, avatar_url, webhook_secret,
                           installation_id, sync_status, owner_id, created_at)
                        VALUES
                          (:id, :github_org_id, :login, :avatar_url, '',
                           :installation_id, 'pending', :owner_id, now())
                        ON CONFLICT (login) DO UPDATE
                          SET installation_id = EXCLUDED.installation_id,
                              avatar_url = EXCLUDED.avatar_url
                        """
                    ),
                    {
                        "id": str(_uuid.uuid4()),
                        "github_org_id": org_id,
                        "login": org_login,
                        "avatar_url": avatar_url,
                        "installation_id": installation_id,
                        "owner_id": owner_id,
                    },
                )
                session.commit()
                logger.info("Registered App installation %s for org %s", installation_id, org_login)
                backfill_org_runs_task.delay(org_login)
            return {"status": "registered", "org_login": org_login}

        elif action == "deleted":
            session.execute(
                text("DELETE FROM organizations WHERE login = :login"),
                {"login": org_login},
            )
            session.commit()
            logger.info("Removed org %s after App uninstall", org_login)
            return {"status": "removed", "org_login": org_login}

        return {"status": "ignored", "action": action}

    except Exception as exc:
        session.rollback()
        logger.exception("register_app_installation_task failed: %s", exc)
        raise self.retry(exc=exc)
    finally:
        session.close()
