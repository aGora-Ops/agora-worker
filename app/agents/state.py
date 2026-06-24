from __future__ import annotations
from typing import TypedDict


class AgentState(TypedDict, total=False):
    repo_owner: str
    repo_name: str
    workflow_file: str
    workflow_yaml: str
    logs: str
    head_sha: str
    run_id: int          # GitHub Actions run id — used by root_cause's get_run_logs tool
    github_token: str
    failure_category: str
    root_cause: str
    root_cause_severity: str
    suggested_yaml: str
    security_risk_score: int
    security_findings: list[str]
    pr_title: str
    pr_description: str
    confidence_score: int        # 0–100 — how confident the AI is the fix is correct
    confidence_reasoning: str    # short explanation of the score
    error: str | None
    agent_trace: list[str]
    fix_examples: list[str]    # accepted fixed_yaml strings from fix_memories, injected as few-shot context
