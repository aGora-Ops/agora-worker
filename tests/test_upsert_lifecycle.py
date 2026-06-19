"""Tests for the lifecycle-aware _upsert_workflow_run function.

A single GitHub Actions run fires multiple webhook deliveries:
  queued -> in_progress -> completed (success or failure)

Each delivery must update the same DB row (ON CONFLICT DO UPDATE), not insert
a duplicate or be silently ignored.
"""
import os
import uuid
from unittest.mock import MagicMock, patch

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql://x:x@localhost/x")
os.environ.setdefault("SECRET_KEY", "test-secret-for-worker")

def _make_session_mock():
    """Return a mock that records the SQL texts passed to execute()."""
    session = MagicMock()
    returning_row = MagicMock()
    returning_row.fetchone.return_value = (str(uuid.uuid4()),)
    session.execute.return_value = returning_row
    return session

def _build_message(run_id: int, action: str, status: str, conclusion=None) -> dict:
    return {
        "run_id": run_id,
        "workflow_id": 999,
        "repo_owner": "acme",
        "repo_name": "widgets",
        "workflow_name": "CI",
        "workflow_file": ".github/workflows/ci.yml",
        "branch": "main",
        "head_sha": "abc123",
        "action": action,
        "status": status,
        "conclusion": conclusion,
        "started_at": "2026-06-18T00:00:00Z",
        "completed_at": "2026-06-18T00:05:00Z" if action == "completed" else None,
        "html_url": "https://github.com/acme/widgets/actions/runs/12345",
    }

class TestUpsertWorkflowRun:
    def test_queued_event_inserts_row(self):
        from app.tasks.remediation import _upsert_workflow_run

        session = _make_session_mock()
        msg = _build_message(12345, "requested", "queued")
        result = _upsert_workflow_run(session, msg)

        assert isinstance(result, uuid.UUID)
        session.execute.assert_called_once()
        sql_text = str(session.execute.call_args.args[0])
        assert "ON CONFLICT" in sql_text
        assert "DO UPDATE" in sql_text

    def test_upsert_sql_coalesces_started_at(self):
        """COALESCE(EXCLUDED.started_at, ...) must preserve an earlier started_at."""
        from app.tasks.remediation import _upsert_workflow_run

        session = _make_session_mock()
        msg = _build_message(12345, "in_progress", "in_progress")
        _upsert_workflow_run(session, msg)

        sql_text = str(session.execute.call_args.args[0])
        assert "COALESCE" in sql_text
        assert "started_at" in sql_text

    def test_completed_failure_sets_conclusion(self):
        from app.tasks.remediation import _upsert_workflow_run

        session = _make_session_mock()
        msg = _build_message(12345, "completed", "completed", conclusion="failure")
        _upsert_workflow_run(session, msg)

        params = session.execute.call_args.args[1]
        assert params["status"] == "completed"
        assert params["conclusion"] == "failure"

    def test_completed_success_sets_conclusion(self):
        from app.tasks.remediation import _upsert_workflow_run

        session = _make_session_mock()
        msg = _build_message(12345, "completed", "completed", conclusion="success")
        _upsert_workflow_run(session, msg)

        params = session.execute.call_args.args[1]
        assert params["conclusion"] == "success"

    def test_null_started_at_on_queued_event(self):
        """Queued events have no started_at; upsert must not crash on None."""
        from app.tasks.remediation import _upsert_workflow_run

        session = _make_session_mock()
        msg = _build_message(12345, "requested", "queued")
        msg["started_at"] = None
        _upsert_workflow_run(session, msg)
        params = session.execute.call_args.args[1]
        assert params["started_at"] is None

    def test_html_url_falls_back_to_constructed_url(self):
        """If html_url is missing, a GitHub URL is constructed from owner/repo/run_id."""
        from app.tasks.remediation import _upsert_workflow_run

        session = _make_session_mock()
        msg = _build_message(12345, "requested", "queued")
        msg["html_url"] = None
        _upsert_workflow_run(session, msg)
        params = session.execute.call_args.args[1]
        assert "acme/widgets/actions/runs/12345" in params["html_url"]

class TestDispatchBranch:
    def test_requires_analysis_true_dispatches_process_failed(self):
        from app.sqs_consumer import _dispatch

        with patch("app.sqs_consumer.process_failed_workflow") as mock_failed, \
             patch("app.sqs_consumer.upsert_workflow_run_task") as mock_upsert:
            _dispatch({"event_type": "workflow_run", "requires_analysis": True, "run_id": 1})
            mock_failed.delay.assert_called_once()
            mock_upsert.delay.assert_not_called()

    def test_requires_analysis_false_dispatches_upsert_only(self):
        from app.sqs_consumer import _dispatch

        with patch("app.sqs_consumer.process_failed_workflow") as mock_failed, \
             patch("app.sqs_consumer.upsert_workflow_run_task") as mock_upsert:
            _dispatch({"event_type": "workflow_run", "requires_analysis": False, "run_id": 1})
            mock_upsert.delay.assert_called_once()
            mock_failed.delay.assert_not_called()

    def test_backfill_org_dispatches_backfill_task(self):
        from app.sqs_consumer import _dispatch

        with patch("app.sqs_consumer.backfill_org_runs_task") as mock_backfill:
            _dispatch({"event_type": "backfill_org", "org_login": "acme"})
            mock_backfill.delay.assert_called_once_with("acme")

    def test_unknown_event_type_does_not_raise(self):
        from app.sqs_consumer import _dispatch
        _dispatch({"event_type": "unknown_event"})
