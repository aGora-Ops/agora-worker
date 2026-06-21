from __future__ import annotations
from typing import TypedDict


class AgentState(TypedDict, total=False):
    repo_owner: str
    repo_name: str
    workflow_file: str
    workflow_yaml: str
    logs: str
    head_sha: str
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
