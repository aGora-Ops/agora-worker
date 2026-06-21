import logging

from mcp import ClientSession
from mcp.client.sse import sse_client

from app.core.config import settings

logger = logging.getLogger(__name__)

# Maps an MCP tool name to the server that implements it. Only read-only GitHub
# tools are exposed to the model — see nodes.py for why write tools
# (branch/commit/PR) are deliberately absent.
_GITHUB_TOOLS = {
    "get_workflow_yaml",
    "get_run_logs",
}


def _server_url_for(tool_name: str) -> str:
    if tool_name in _GITHUB_TOOLS:
        return settings.MCP_GITHUB_URL
    raise ValueError(f"Unknown MCP tool '{tool_name}'")


async def call_tool(tool_name: str, params: dict) -> str:
    """Call an MCP tool by name over in-cluster SSE and return its result as text.

    Uses the official MCP Python SDK (mcp.client.sse). The model decides to call
    a tool (Converse tool-use); the worker bridges that call to the in-cluster
    MCP server here and feeds the result back.
    """
    url = _server_url_for(tool_name)
    async with sse_client(url) as (read_stream, write_stream):
        async with ClientSession(read_stream, write_stream) as session:
            await session.initialize()
            result = await session.call_tool(tool_name, arguments=params)

    parts = []
    for block in getattr(result, "content", None) or []:
        text = getattr(block, "text", None)
        parts.append(text if text is not None else str(block))
    return "\n".join(parts) if parts else str(result)
