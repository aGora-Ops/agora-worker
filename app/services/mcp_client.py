import logging

from fastmcp import Client

from app.core.config import settings

logger = logging.getLogger(__name__)

# Maps a Bedrock action group function name to the MCP server that implements it.
# Only read-only tools are exposed to agents — see nodes.py for why write
# tools (branch/commit/PR) are deliberately absent here.
_GITHUB_TOOLS = {
    "get_workflow_yaml",
    "get_run_logs",
}
_AWS_TOOLS = {
    "get_cloudwatch_logs",
    "get_sqs_queue_depth",
    "describe_ecr_image",
}


def _server_url_for(tool_name: str) -> str:
    if tool_name in _GITHUB_TOOLS:
        return settings.MCP_GITHUB_URL
    if tool_name in _AWS_TOOLS:
        return settings.MCP_AWS_URL
    raise ValueError(f"Unknown MCP tool '{tool_name}'")


async def call_tool(tool_name: str, params: dict) -> str:
    """Call an MCP tool by name over in-cluster SSE and return its result as text."""
    url = _server_url_for(tool_name)
    async with Client(url) as client:
        result = await client.call_tool(tool_name, params)
    if hasattr(result, "content"):
        return "\n".join(getattr(block, "text", str(block)) for block in result.content)
    return str(result)
