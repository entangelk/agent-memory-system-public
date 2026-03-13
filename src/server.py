import asyncio
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent

from src import config
from src.tools import (
    memory_save,
    memory_recall,
    memory_summarize,
    session_digest,
    memory_approve,
    memory_compact,
    memory_update,
    memory_delete,
    topic_lookup,
    memory_policy,
)
from src.resources import memory_resources
from src.db.connection import close_connection

app = Server("agent-memory")

# Register resources.
memory_resources.register(app)

# Register tool modules in a fixed order.
_TOOL_MODULES = [memory_save, memory_recall, memory_summarize, session_digest, memory_approve, memory_compact, memory_update, memory_delete, topic_lookup, memory_policy]


@app.list_tools()
async def list_tools() -> list[Tool]:
    tools: list[Tool] = []
    for mod in _TOOL_MODULES:
        tools.extend(mod.get_tools())
    return tools


@app.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    for mod in _TOOL_MODULES:
        result = await mod.handle(name, arguments)
        if result is not None:
            return result
    raise ValueError(f"Unknown tool: {name}")


async def main() -> None:
    if config.PRELOAD_EMBEDDING_MODEL:
        # Optional preload to reduce first recall latency after the model is already cached.
        from src.engine.chroma_engine import chroma_enabled, _get_model
        if chroma_enabled():
            await asyncio.to_thread(_get_model)

    async with stdio_server() as (read_stream, write_stream):
        try:
            await app.run(read_stream, write_stream, app.create_initialization_options())
        finally:
            await close_connection()


if __name__ == "__main__":
    asyncio.run(main())
