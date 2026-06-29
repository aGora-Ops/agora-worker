"""Regression tests for the optional MCP root-cause enrichment path."""
import os
from unittest.mock import patch

os.environ.setdefault("DATABASE_URL", "postgresql://x:x@localhost/x")
os.environ.setdefault("SECRET_KEY", "test-secret-only")


def _state() -> dict:
    return {
        "repo_owner": "Stagecraft-Ops",
        "repo_name": "stagecraft-api",
        "workflow_file": ".github/workflows/ci.yml",
        "workflow_yaml": "name: CI\njobs: {}",
        "logs": "Python version 99 was not found",
        "head_sha": "abc123",
        "run_id": 42,
        "failure_category": "DEPENDENCY_VERSION",
        "agent_trace": [],
    }


def test_root_cause_uses_direct_converse_when_mcp_is_disabled():
    from app.agents import nodes

    with patch.object(nodes.settings, "USE_MCP_TOOLS", False), \
         patch.object(
             nodes,
             "_converse",
             return_value='{"root_cause":"Python 99 is invalid","severity":"low"}',
         ) as converse, \
         patch.object(nodes, "_converse_with_tools") as with_tools:
        result = nodes.analyse_root_cause(_state())

    assert result["root_cause"] == "Python 99 is invalid"
    assert result["root_cause_severity"] == "low"
    converse.assert_called_once()
    with_tools.assert_not_called()


def test_mcp_tool_schemas_match_github_mcp_server():
    from app.agents.nodes import _ROOT_CAUSE_TOOLCONFIG

    tools = {
        tool["toolSpec"]["name"]: tool["toolSpec"]["inputSchema"]["json"]
        for tool in _ROOT_CAUSE_TOOLCONFIG["tools"]
    }
    assert tools["get_run_logs"]["required"] == ["owner", "repo", "run_id"]
    assert tools["get_workflow_yaml"]["required"] == ["owner", "repo", "path", "ref"]
