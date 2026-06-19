from __future__ import annotations
from typing import TypedDict


class AgentState(TypedDict, total=False):
    repo_owner: str
    repo_name: str
    workflow_file: str
    workflow_yaml: str
    logs: str
    failure_category: str
    root_cause: str
    root_cause_severity: str
    suggested_yaml: str
    security_risk_score: int
    security_findings: list[str]
    pr_title: str
    pr_description: str
    error: str | None
    agent_trace: list[str]
