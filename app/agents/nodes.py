import json
import logging
import uuid

import boto3

from app.core.config import settings
from app.agents.state import AgentState

logger = logging.getLogger(__name__)


def _invoke_bedrock_agent(agent_alias_id: str, session_id: str, prompt: str) -> str:
    client = boto3.client("bedrock-agent-runtime", region_name=settings.AWS_REGION)
    response = client.invoke_agent(
        agentId=settings.BEDROCK_AGENT_IDS.get(agent_alias_id, ""),
        agentAliasId=settings.BEDROCK_AGENT_ALIAS_IDS.get(agent_alias_id, "TSTALIASID"),
        sessionId=session_id,
        inputText=prompt,
    )
    completion = ""
    for event in response["completion"]:
        if "chunk" in event:
            completion += event["chunk"]["bytes"].decode()
    return completion.strip()


def classify_failure(state: AgentState) -> AgentState:
    session_id = str(uuid.uuid4())
    prompt = (
        f"Workflow file: {state['workflow_file']}\n\n"
        f"Failure logs (scrubbed):\n{state['logs'][:4000]}\n\n"
        "Classify the failure. Respond with exactly one of: "
        "DEPENDENCY_VERSION | AUTH_FAILURE | NETWORK_TIMEOUT | CONFIG_ERROR | "
        "TEST_FAILURE | BUILD_ERROR | LINT_ERROR | PERMISSION_ERROR | UNKNOWN"
    )
    category = _invoke_bedrock_agent("classifier", session_id, prompt)
    valid = {"DEPENDENCY_VERSION", "AUTH_FAILURE", "NETWORK_TIMEOUT", "CONFIG_ERROR",
             "TEST_FAILURE", "BUILD_ERROR", "LINT_ERROR", "PERMISSION_ERROR", "UNKNOWN"}
    category = category.upper().strip() if category.upper().strip() in valid else "UNKNOWN"
    trace = state.get("agent_trace", [])
    trace.append(f"classify_failure → {category}")
    return {**state, "failure_category": category, "agent_trace": trace}


def analyse_root_cause(state: AgentState) -> AgentState:
    session_id = str(uuid.uuid4())
    prompt = (
        f"Failure category: {state['failure_category']}\n\n"
        f"Workflow YAML:\n{state['workflow_yaml'][:3000]}\n\n"
        f"Logs:\n{state['logs'][:4000]}\n\n"
        "Identify the specific root cause. Respond in JSON: "
        '{"root_cause": "...", "severity": "low|medium|high|critical"}'
    )
    raw = _invoke_bedrock_agent("root_cause", session_id, prompt)
    try:
        parsed = json.loads(raw)
        root_cause = parsed.get("root_cause", raw)
        severity = parsed.get("severity", "medium")
    except json.JSONDecodeError:
        root_cause = raw
        severity = "medium"
    trace = state.get("agent_trace", [])
    trace.append(f"analyse_root_cause → severity={severity}")
    return {**state, "root_cause": root_cause, "root_cause_severity": severity, "agent_trace": trace}


def generate_fix(state: AgentState) -> AgentState:
    session_id = str(uuid.uuid4())
    prompt = (
        f"Root cause: {state['root_cause']}\n\n"
        f"Original workflow YAML:\n{state['workflow_yaml']}\n\n"
        "Generate the corrected YAML. Return ONLY the complete, valid YAML — no commentary, "
        "no markdown fences."
    )
    fixed_yaml = _invoke_bedrock_agent("yaml_fixer", session_id, prompt)
    if fixed_yaml.startswith("```"):
        lines = fixed_yaml.splitlines()
        fixed_yaml = "\n".join(l for l in lines if not l.startswith("```")).strip()
    trace = state.get("agent_trace", [])
    trace.append("generate_fix → suggested_yaml produced")
    return {**state, "suggested_yaml": fixed_yaml, "agent_trace": trace}


def review_security(state: AgentState) -> AgentState:
    session_id = str(uuid.uuid4())
    prompt = (
        f"Proposed workflow YAML fix:\n{state['suggested_yaml']}\n\n"
        "Review for security issues. Check: hardcoded secrets, missing SHA pins on actions, "
        "overbroad permissions, dangerous shell commands, untrusted registries.\n"
        'Respond in JSON: {"risk_score": 0-10, "findings": ["finding1", ...]}'
    )
    raw = _invoke_bedrock_agent("security_reviewer", session_id, prompt)
    try:
        parsed = json.loads(raw)
        risk_score = int(parsed.get("risk_score", 0))
        findings = parsed.get("findings", [])
    except (json.JSONDecodeError, ValueError):
        risk_score = 0
        findings = []
    trace = state.get("agent_trace", [])
    trace.append(f"review_security → risk_score={risk_score}, findings={len(findings)}")
    return {**state, "security_risk_score": risk_score, "security_findings": findings, "agent_trace": trace}


def write_pr_description(state: AgentState) -> AgentState:
    session_id = str(uuid.uuid4())
    findings_text = "\n".join(f"- {f}" for f in state.get("security_findings", [])) or "None identified"
    prompt = (
        f"Root cause: {state['root_cause']}\n"
        f"Failure category: {state['failure_category']}\n"
        f"Security findings: {findings_text}\n\n"
        "Write a concise GitHub PR title and body for the AI-suggested fix. "
        'Respond in JSON: {"title": "fix: ...", "body": "## Root Cause\\n..."}'
    )
    raw = _invoke_bedrock_agent("pr_writer", session_id, prompt)
    try:
        parsed = json.loads(raw)
        pr_title = parsed.get("title", f"fix: AI remediation for {state['workflow_file']}")
        pr_description = parsed.get("body", f"## Root Cause\n{state['root_cause']}")
    except json.JSONDecodeError:
        pr_title = f"fix: AI remediation for {state['workflow_file']}"
        pr_description = f"## Root Cause\n{state['root_cause']}"
    trace = state.get("agent_trace", [])
    trace.append("write_pr_description → done")
    return {**state, "pr_title": pr_title, "pr_description": pr_description, "agent_trace": trace}


def should_block_high_risk(state: AgentState) -> str:
    if state.get("security_risk_score", 0) >= 8:
        return "block"
    return "approve"
