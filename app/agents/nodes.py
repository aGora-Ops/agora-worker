import asyncio
import json
import logging
import re
import time
import uuid

import boto3

from app.core.config import settings
from app.agents.state import AgentState
from app.services import mcp_client
from app.services.bedrock_client import _bedrock_boto3_kwargs

logger = logging.getLogger(__name__)

_MAX_RETRIES = 2
_MAX_ROC_ROUNDS = 6

# Read-only GitHub tools exposed to agents need the user's OAuth token
# injected — the Bedrock action group schema has no notion of "current user".
# Write tools (branch/commit/PR) are deliberately NOT exposed to any agent:
# the worker is read-only with respect to GitHub by design (see
# tasks/remediation.py) so a prompt-injected log can at worst produce a bad
# suggestion, never an autonomous write to the user's repo.
_GITHUB_TOOLS = {
    "get_workflow_yaml",
    "get_run_logs",
}


def _invoke_bedrock_agent(agent_key: str, session_id: str, prompt: str, github_token: str | None = None) -> str:
    """Invoke a Bedrock agent, resolving any Return-of-Control tool calls via MCP.

    The agent may stream a `returnControl` event instead of a final completion when
    its action group decides a tool needs to run. We execute that tool against the
    in-cluster MCP server and re-invoke the agent with the result, looping until the
    agent returns a normal completion.
    """
    client = boto3.client(
        "bedrock-agent-runtime",
        region_name=settings.AWS_REGION,
        **_bedrock_boto3_kwargs(),
    )
    agent_id = settings.BEDROCK_AGENT_IDS.get(agent_key, "")
    agent_alias_id = settings.BEDROCK_AGENT_ALIAS_IDS.get(agent_key, "TSTALIASID")

    session_state: dict | None = None
    for _round in range(_MAX_ROC_ROUNDS):
        for attempt in range(_MAX_RETRIES + 1):
            try:
                kwargs = {"agentId": agent_id, "agentAliasId": agent_alias_id, "sessionId": session_id}
                if session_state is not None:
                    kwargs["sessionState"] = session_state
                else:
                    kwargs["inputText"] = prompt
                response = client.invoke_agent(**kwargs)
                break
            except client.exceptions.ThrottlingException:
                if attempt < _MAX_RETRIES:
                    time.sleep(2 ** (attempt + 1))
                    continue
                raise

        completion = ""
        return_control = None
        for event in response["completion"]:
            if "chunk" in event:
                completion += event["chunk"]["bytes"].decode()
            elif "returnControl" in event:
                return_control = event["returnControl"]

        if return_control is None:
            return completion.strip()

        results = []
        for invocation_input in return_control["invocationInputs"]:
            func_input = invocation_input["functionInvocationInput"]
            function_name = func_input["function"]
            action_group = func_input["actionGroup"]
            params = {p["name"]: p["value"] for p in func_input.get("parameters", [])}
            if github_token and function_name in _GITHUB_TOOLS:
                params["github_token"] = github_token

            try:
                tool_result = asyncio.run(mcp_client.call_tool(function_name, params))
            except Exception as exc:
                logger.warning("MCP tool %s failed: %s", function_name, exc)
                tool_result = f"ERROR: {exc}"

            results.append({
                "functionResult": {
                    "actionGroup": action_group,
                    "function": function_name,
                    "responseBody": {"TEXT": {"body": tool_result}},
                }
            })

        session_state = {
            "invocationId": return_control["invocationId"],
            "returnControlInvocationResults": results,
        }

    raise RuntimeError(f"Agent '{agent_key}' exceeded {_MAX_ROC_ROUNDS} Return-of-Control rounds")


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
        f"Repository: {state['repo_owner']}/{state['repo_name']}\n"
        f"Failure category: {state['failure_category']}\n\n"
        f"Workflow YAML:\n{state['workflow_yaml'][:3000]}\n\n"
        f"Logs:\n{state['logs'][:4000]}\n\n"
        "Identify the specific root cause. Use the github-tools and aws-diagnostics action "
        "groups if you need more context. Respond in JSON: "
        '{"root_cause": "...", "severity": "low|medium|high|critical"}'
    )
    raw = _invoke_bedrock_agent("root_cause", session_id, prompt, github_token=state.get("github_token"))
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


def _looks_like_yaml(text: str) -> bool:
    """Heuristic: return True when the text looks like YAML rather than English prose.

    Bedrock Agents sometimes refuse to produce YAML and instead return a capability
    disclaimer like "The agent does not have the capability to directly modify the
    YAML file".  We detect this by checking that the response contains at least one
    YAML key-value line (``key: value`` or ``key:`` alone on a line) within the first
    20 lines and does NOT start with a long English sentence.
    """
    lines = [l for l in text.strip().splitlines() if l.strip()][:20]
    if not lines:
        return False
    # A plain English disclaimer will usually start with a capital word and no colon in pos 1-30
    first = lines[0].strip()
    if first and first[0].isupper() and ":" not in first[:40] and not first.startswith("name"):
        return False
    yaml_key_pattern = re.compile(r"^\s*[\w\-\.]+\s*:")
    matching = sum(1 for l in lines if yaml_key_pattern.match(l))
    return matching >= 2


def generate_fix(state: AgentState) -> AgentState:
    session_id = str(uuid.uuid4())
    prompt = (
        f"Repository: {state['repo_owner']}/{state['repo_name']}\n"
        f"Root cause: {state['root_cause']}\n\n"
        f"Original workflow YAML:\n{state['workflow_yaml']}\n\n"
        "You MUST produce the complete corrected workflow YAML. Rules:\n"
        "- If a tool/language version is invalid (e.g. python-version: '99'), replace it with "
        "the latest stable version (e.g. '3.12' for Python, '20' for Node.js, '21' for Java).\n"
        "- Make the minimal change needed to fix the root cause.\n"
        "- Return ONLY the complete, valid YAML — no prose, no markdown fences, no explanations.\n"
        "- If you are uncertain about a version, use the most recent LTS/stable release.\n"
        "- Never say you cannot determine something — always produce the corrected YAML."
    )
    fixed_yaml = _invoke_bedrock_agent("yaml_fixer", session_id, prompt, github_token=state.get("github_token"))
    if fixed_yaml.startswith("```"):
        lines = fixed_yaml.splitlines()
        fixed_yaml = "\n".join(l for l in lines if not l.startswith("```")).strip()

    trace = state.get("agent_trace", [])

    if not _looks_like_yaml(fixed_yaml):
        # The agent returned a capability refusal or English prose instead of YAML.
        # Fall back to calling the Bedrock Converse API directly.
        logger.warning(
            "yaml_fixer agent returned non-YAML response (capability refusal?). "
            "Falling back to direct Bedrock Converse API. Agent response was: %r",
            fixed_yaml[:200],
        )
        trace.append("generate_fix → agent refused, falling back to Converse API")
        from app.services.bedrock_client import BedrockRemediationClient
        fallback = BedrockRemediationClient()
        fixed_yaml = fallback.generate_yaml_fix(
            workflow_yaml=state["workflow_yaml"],
            root_cause=state["root_cause"],
        )

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
