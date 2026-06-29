import asyncio
import json
import logging
import time
import uuid

import boto3
import yaml

from app.core.config import settings
from app.agents.state import AgentState
from app.services import mcp_client
from app.services.bedrock_client import _bedrock_boto3_kwargs

logger = logging.getLogger(__name__)

_MAX_RETRIES = 2
_MAX_TOOL_ROUNDS = 6

# Read-only GitHub tools routed to stagecraft-mcp-github. Write tools are NOT exposed
# here — PRs are raised only via the api-service after human review.
_GITHUB_TOOLS = {"get_workflow_yaml", "get_run_logs"}

# ---------------------------------------------------------------------------
# Bedrock Converse helpers
# ---------------------------------------------------------------------------

def _bedrock_client():
    return boto3.client(
        "bedrock-runtime",
        region_name=settings.AWS_REGION,
        **_bedrock_boto3_kwargs(),
    )


def _converse(prompt: str, max_tokens: int = 2048, temperature: float = 0.0) -> str:
    """Single-turn Bedrock Converse call. Retries on throttle."""
    client = _bedrock_client()
    for attempt in range(_MAX_RETRIES + 1):
        try:
            response = client.converse(
                modelId=settings.BEDROCK_MODEL_ID,
                messages=[{"role": "user", "content": [{"text": prompt}]}],
                inferenceConfig={"maxTokens": max_tokens, "temperature": temperature},
            )
            return response["output"]["message"]["content"][0]["text"].strip()
        except client.exceptions.ThrottlingException:
            if attempt < _MAX_RETRIES:
                time.sleep(2 ** (attempt + 1))
                continue
            raise


def _converse_with_tools(
    prompt: str,
    tool_config: dict,
    github_token: str | None = None,
    max_tokens: int = 4096,
) -> str:
    """Agentic Converse loop with tool-use.

    Calls the model with toolConfig; if the model wants to call a tool,
    we route it to the matching MCP server via mcp_client.call_tool, then
    feed the result back. Loops until a final text response or _MAX_TOOL_ROUNDS
    is exhausted. MCP failures are caught and fed back as error strings so a
    flaky tool never stalls the pipeline.
    """
    client = _bedrock_client()
    messages = [{"role": "user", "content": [{"text": prompt}]}]
    assistant_content: list = []

    for _round in range(_MAX_TOOL_ROUNDS):
        for attempt in range(_MAX_RETRIES + 1):
            try:
                response = client.converse(
                    modelId=settings.BEDROCK_MODEL_ID,
                    messages=messages,
                    toolConfig=tool_config,
                    inferenceConfig={"maxTokens": max_tokens},
                )
                break
            except client.exceptions.ThrottlingException:
                if attempt < _MAX_RETRIES:
                    time.sleep(2 ** (attempt + 1))
                    continue
                raise

        stop_reason = response.get("stopReason", "")
        assistant_content = response["output"]["message"]["content"]
        messages.append({"role": "assistant", "content": assistant_content})

        if stop_reason != "tool_use":
            for block in assistant_content:
                if "text" in block:
                    return block["text"].strip()
            return ""

        tool_results = []
        for block in assistant_content:
            if "toolUse" not in block:
                continue
            tool_use = block["toolUse"]
            tool_name = tool_use["name"]
            tool_input = dict(tool_use.get("input", {}))
            tool_use_id = tool_use["toolUseId"]

            if github_token and tool_name in _GITHUB_TOOLS:
                tool_input["github_token"] = github_token

            try:
                result_text = asyncio.run(mcp_client.call_tool(tool_name, tool_input))
                logger.info("MCP tool %s succeeded (%d chars)", tool_name, len(result_text))
            except Exception as exc:
                logger.warning("MCP tool %s failed: %s", tool_name, exc)
                result_text = f"ERROR calling {tool_name}: {exc}"

            tool_results.append({
                "toolResult": {
                    "toolUseId": tool_use_id,
                    "content": [{"text": result_text}],
                }
            })

        messages.append({"role": "user", "content": tool_results})

    logger.warning("_converse_with_tools: hit max rounds (%d), returning last text", _MAX_TOOL_ROUNDS)
    for block in assistant_content:
        if "text" in block:
            return block["text"].strip()
    return ""


def _parse_json(raw: str) -> dict:
    """Extract JSON from a model response that may have prose or fences around it."""
    stripped = raw.strip()
    if stripped.startswith("```"):
        inner = "\n".join(l for l in stripped.splitlines() if not l.strip().startswith("```")).strip()
        try:
            return json.loads(inner)
        except json.JSONDecodeError:
            pass
    try:
        return json.loads(stripped)
    except json.JSONDecodeError:
        pass
    start = stripped.find("{")
    end = stripped.rfind("}")
    if start != -1 and end > start:
        try:
            return json.loads(stripped[start:end + 1])
        except json.JSONDecodeError:
            pass
    return {}


# Tool config for root_cause — model may call get_run_logs or get_workflow_yaml
# via the in-cluster stagecraft-mcp-github SSE server.
_ROOT_CAUSE_TOOLCONFIG = {
    "tools": [
        {
            "toolSpec": {
                "name": "get_run_logs",
                "description": (
                    "Fetch the full failure logs for a GitHub Actions workflow run. "
                    "Use when the truncated logs in the prompt are insufficient to determine root cause."
                ),
                "inputSchema": {
                    "json": {
                        "type": "object",
                        "properties": {
                            "owner": {"type": "string", "description": "GitHub org or user"},
                            "repo": {"type": "string", "description": "Repository name"},
                            "run_id": {"type": "integer", "description": "GitHub Actions run ID"},
                        },
                        "required": ["owner", "repo", "run_id"],
                    }
                },
            }
        },
        {
            "toolSpec": {
                "name": "get_workflow_yaml",
                "description": (
                    "Fetch the raw workflow YAML file from GitHub. "
                    "Use when you need to inspect the full workflow definition."
                ),
                "inputSchema": {
                    "json": {
                        "type": "object",
                        "properties": {
                            "owner": {"type": "string", "description": "GitHub org or user"},
                            "repo": {"type": "string", "description": "Repository name"},
                            "path": {
                                "type": "string",
                                "description": "Workflow file path (e.g. .github/workflows/ci.yml)",
                            },
                            "ref": {
                                "type": "string",
                                "description": "Commit SHA or branch from which to read the workflow",
                            },
                        },
                        "required": ["owner", "repo", "path", "ref"],
                    }
                },
            }
        },
    ]
}


# ---------------------------------------------------------------------------
# Pipeline nodes
# ---------------------------------------------------------------------------

def classify_failure(state: AgentState) -> AgentState:
    prompt = (
        f"Workflow file: {state['workflow_file']}\n\n"
        f"Failure logs (scrubbed):\n{state['logs'][:4000]}\n\n"
        "Classify the failure. Respond with exactly one of: "
        "DEPENDENCY_VERSION | AUTH_FAILURE | NETWORK_TIMEOUT | CONFIG_ERROR | "
        "TEST_FAILURE | BUILD_ERROR | LINT_ERROR | PERMISSION_ERROR | UNKNOWN"
    )
    raw = _converse(prompt, max_tokens=32)
    valid = {"DEPENDENCY_VERSION", "AUTH_FAILURE", "NETWORK_TIMEOUT", "CONFIG_ERROR",
             "TEST_FAILURE", "BUILD_ERROR", "LINT_ERROR", "PERMISSION_ERROR", "UNKNOWN"}
    category = raw.upper().strip() if raw.upper().strip() in valid else "UNKNOWN"
    trace = state.get("agent_trace", [])
    trace.append(f"classify_failure → {category}")
    return {**state, "failure_category": category, "agent_trace": trace}


def analyse_root_cause(state: AgentState) -> AgentState:
    prompt = (
        f"Repository: {state['repo_owner']}/{state['repo_name']}\n"
        f"Run ID: {state.get('run_id', '')}\n"
        f"Failure category: {state['failure_category']}\n\n"
        f"Workflow YAML:\n{state['workflow_yaml'][:3000]}\n\n"
        f"Logs:\n{state['logs'][:4000]}\n\n"
        "Identify the specific root cause. If MCP enrichment is enabled and the supplied "
        "context is insufficient, call get_run_logs with owner, repo, and run_id; or call "
        "get_workflow_yaml with owner, repo, path, and ref. "
        'Respond in JSON: {"root_cause": "...", "severity": "low|medium|high|critical"}'
    )
    if settings.USE_MCP_TOOLS:
        raw = _converse_with_tools(
            prompt,
            tool_config=_ROOT_CAUSE_TOOLCONFIG,
            github_token=state.get("github_token"),
            max_tokens=1024,
        )
    else:
        logger.info("MCP enrichment disabled; using fetched workflow and log context")
        raw = _converse(prompt, max_tokens=1024)
    parsed = _parse_json(raw)
    root_cause = parsed.get("root_cause", raw) if parsed else raw
    severity = parsed.get("severity", "medium") if parsed else "medium"
    trace = state.get("agent_trace", [])
    trace.append(f"analyse_root_cause → severity={severity}")
    return {**state, "root_cause": root_cause, "root_cause_severity": severity, "agent_trace": trace}


def _strip_fences(text: str) -> str:
    t = text.strip()
    if t.startswith("```"):
        t = "\n".join(l for l in t.splitlines() if not l.strip().startswith("```")).strip()
    return t


def _validate_fix(original: str, fixed: str) -> tuple[bool, str]:
    if not fixed or not fixed.strip():
        return False, "empty output"
    try:
        parsed = yaml.safe_load(fixed)
    except yaml.YAMLError:
        return False, "invalid YAML syntax"
    if not isinstance(parsed, dict) or "jobs" not in parsed:
        return False, "not a GitHub Actions workflow (no jobs:)"
    if fixed.strip() == original.strip():
        return False, "no change from original"
    return True, "ok"


def generate_fix(state: AgentState) -> AgentState:
    from app.services.bedrock_client import BedrockRemediationClient
    import yaml

    trace = state.get("agent_trace", [])
    client = BedrockRemediationClient()
    original = state["workflow_yaml"]

    fixed = ""
    last_error_context = "unknown"
    candidate = None

    # Try up to 3 times (1 initial attempt + 2 correction attempts)
    for attempt in range(3):
        if attempt == 0:
            logger.info("Attempt 1: Generating initial YAML fix from Bedrock")
            try:
                raw_candidate = client.generate_yaml_fix(
                    workflow_yaml=original,
                    root_cause=state["root_cause"],
                    failure_category=state.get("failure_category", "UNKNOWN"),
                    logs=state.get("logs", ""),
                )
            except Exception as exc:
                last_error_context = f"Bedrock invocation failed: {str(exc)}"
                logger.warning(f"generate_fix attempt {attempt + 1} Bedrock error: {last_error_context}")
                continue
        else:
            logger.info(f"Attempt {attempt + 1}: Self-correcting malformed YAML fix")
            try:
                raw_candidate = client.correct_yaml_syntax(
                    original_yaml=original,
                    malformed_yaml=candidate,
                    error_message=last_error_context
                )
            except Exception as exc:
                last_error_context = f"Bedrock correction invocation failed: {str(exc)}"
                logger.warning(f"generate_fix attempt {attempt + 1} Bedrock error: {last_error_context}")
                continue

        candidate = _strip_fences(raw_candidate)
        
        # Post-process to fix common formatting anomalies where colons lack trailing space
        import re
        lines = []
        spacing_pattern = re.compile(r"^([ \t]*[a-zA-Z0-9_-]+):(?!\s)(.+)$")
        for line in candidate.splitlines():
            m = spacing_pattern.match(line)
            if m:
                lines.append(f"{m.group(1)}: {m.group(2)}")
            else:
                lines.append(line)
        candidate = "\n".join(lines)

        # Validate the candidate
        if not candidate or not candidate.strip():
            last_error_context = "Validation failed: Suggested YAML is empty."
            logger.warning(f"generate_fix attempt {attempt + 1} validation failed: {last_error_context}")
            continue

        if candidate.strip() == original.strip():
            last_error_context = "Validation failed: Suggested YAML is identical to the original workflow; no change was made."
            logger.warning(f"generate_fix attempt {attempt + 1} validation failed: {last_error_context}")
            continue

        try:
            parsed = yaml.safe_load(candidate)
            if not isinstance(parsed, dict) or "jobs" not in parsed:
                last_error_context = "Validation failed: Root key 'jobs' was not found in the generated workflow YAML."
                logger.warning(f"generate_fix attempt {attempt + 1} validation failed: {last_error_context}")
                continue

            # If all checks pass, we have our valid YAML!
            fixed = candidate
            break

        except yaml.YAMLError as exc:
            last_error_context = f"YAML syntax / parsing error:\n{str(exc)}"
            logger.warning(f"generate_fix attempt {attempt + 1} validation failed with YAMLError: {last_error_context}")

    if not fixed:
        trace.append("generate_fix → self-correction loop failed to produce valid YAML")
        return {**state, "suggested_yaml": None, "agent_trace": trace}

    trace.append(f"generate_fix → suggested_yaml produced on attempt {attempt + 1} (validated)")
    return {**state, "suggested_yaml": fixed, "agent_trace": trace}


def review_security(state: AgentState) -> AgentState:
    trace = state.get("agent_trace", [])
    if not state.get("suggested_yaml"):
        trace.append("review_security → skipped (no fix)")
        return {**state, "security_risk_score": 0, "security_findings": [], "agent_trace": trace}

    prompt = (
        f"Proposed workflow YAML fix:\n{state['suggested_yaml']}\n\n"
        "Review for security issues. Check: hardcoded secrets, missing SHA pins on actions, "
        "overbroad permissions, dangerous shell commands, untrusted registries.\n"
        'Respond in JSON: {"risk_score": 0-10, "findings": ["finding1", ...]}'
    )
    raw = _converse(prompt, max_tokens=512)
    parsed = _parse_json(raw)
    try:
        risk_score = int(parsed.get("risk_score", 0))
        findings = parsed.get("findings", [])
    except (ValueError, AttributeError):
        risk_score = 0
        findings = []
    trace.append(f"review_security → risk_score={risk_score}, findings={len(findings)}")
    return {**state, "security_risk_score": risk_score, "security_findings": findings, "agent_trace": trace}


def write_pr_description(state: AgentState) -> AgentState:
    findings_text = "\n".join(f"- {f}" for f in state.get("security_findings", [])) or "None identified"
    prompt = (
        f"Root cause: {state['root_cause']}\n"
        f"Failure category: {state['failure_category']}\n"
        f"Security findings: {findings_text}\n\n"
        "Write a concise GitHub PR title and body for the AI-suggested fix. "
        'Respond in JSON: {"title": "fix: ...", "body": "## Root Cause\\n..."}'
    )
    raw = _converse(prompt, max_tokens=512)
    parsed = _parse_json(raw)
    pr_title = (
        parsed.get("title", f"fix: AI remediation for {state['workflow_file']}")
        if parsed else f"fix: AI remediation for {state['workflow_file']}"
    )
    pr_description = (
        parsed.get("body", f"## Root Cause\n{state['root_cause']}")
        if parsed else f"## Root Cause\n{state['root_cause']}"
    )
    trace = state.get("agent_trace", [])
    trace.append("write_pr_description → done")
    return {**state, "pr_title": pr_title, "pr_description": pr_description, "agent_trace": trace}


def should_block_high_risk(state: AgentState) -> str:
    if state.get("security_risk_score", 0) >= 8:
        return "block"
    return "approve"


def score_confidence(state: AgentState) -> AgentState:
    from app.services.bedrock_client import BedrockRemediationClient

    trace = state.get("agent_trace", [])
    suggested = state.get("suggested_yaml", "")

    if not suggested or state.get("error"):
        trace.append("score_confidence → 0 (no suggested YAML or pipeline error)")
        return {**state, "confidence_score": 0, "confidence_reasoning": "No fix was produced.", "agent_trace": trace}

    findings_text = "; ".join(state.get("security_findings", [])) or "none"

    prompt = (
        "You are a senior DevOps engineer reviewing an AI-generated GitHub Actions YAML fix.\n\n"
        f"FAILURE CATEGORY: {state.get('failure_category', 'UNKNOWN')}\n"
        f"ROOT CAUSE: {state.get('root_cause', '')}\n"
        f"SECURITY RISK SCORE: {state.get('security_risk_score', 0)}/10\n"
        f"SECURITY FINDINGS: {findings_text}\n\n"
        "ORIGINAL FAILING YAML:\n"
        f"{state.get('workflow_yaml', '')[:2000]}\n\n"
        "SUGGESTED FIX YAML:\n"
        f"{suggested[:2000]}\n\n"
        "Score your confidence that this fix correctly resolves the root cause WITHOUT introducing new problems.\n"
        "Consider: Does the fix directly address the root cause? Is it minimal? Are there any risks?\n\n"
        'Respond ONLY with JSON: {"score": <0-100>, "reasoning": "<one sentence>"}\n'
        "score=90-100: fix is clearly correct and minimal.\n"
        "score=70-89: likely correct but has minor uncertainty.\n"
        "score=50-69: fix is plausible but incomplete or broad.\n"
        "score=0-49: fix is wrong, risky, or doesn't address root cause."
    )

    try:
        client_obj = BedrockRemediationClient()
        response = client_obj._client.converse(
            modelId=client_obj._model_id,
            messages=[{"role": "user", "content": [{"text": prompt}]}],
            inferenceConfig={"maxTokens": 256},
        )
        raw: str = response["output"]["message"]["content"][0]["text"].strip()
        if raw.startswith("```"):
            raw = "\n".join(l for l in raw.splitlines() if not l.startswith("```")).strip()
        parsed = json.loads(raw)
        score = max(0, min(100, int(parsed.get("score", 50))))
        reasoning = parsed.get("reasoning", "")
    except Exception as exc:
        logger.warning("score_confidence failed, using heuristic: %s", exc)
        risk = state.get("security_risk_score", 0)
        score = max(10, 80 - (risk * 5))
        reasoning = "Heuristic score (Bedrock unavailable)."

    trace.append(f"score_confidence → {score}/100")
    return {**state, "confidence_score": score, "confidence_reasoning": reasoning, "agent_trace": trace}
