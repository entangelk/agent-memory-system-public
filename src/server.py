import asyncio
from contextlib import asynccontextmanager

import uvicorn
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
from mcp.types import GetPromptResult, Prompt, PromptArgument, PromptMessage, Tool, TextContent
from starlette.applications import Starlette
from starlette.routing import Route
from starlette.types import Receive, Scope, Send

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
_MEMORY_TOOL_GUIDE_PROMPT = "memory_tool_guide"


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


def _build_memory_tool_guide_text(user_request: str) -> str:
    lines = [
        "Use the memory tools deliberately.",
        "If the user asks what you remember, refers to previous conversations, or asks about past preferences, plans, facts, or events, call `memory_recall` before answering from memory.",
        "If you learn a stable preference, fact, plan, or notable event worth keeping, save a concise distilled memory with `memory_save`.",
        "If a conversation is long and contains several candidate memories, use `session_digest` instead of saving raw conversation text.",
        "Follow `memory_policy`, and only include sensitive details when the user's request is explicit.",
    ]
    if user_request:
        lines.insert(0, f"Current user request: {user_request}")
    return "\n".join(lines)


@app.list_prompts()
async def list_prompts() -> list[Prompt]:
    return [
        Prompt(
            name=_MEMORY_TOOL_GUIDE_PROMPT,
            description="Guidance for memory-aware assistants using recall/save tools in this MCP server.",
            arguments=[
                PromptArgument(
                    name="user_request",
                    description="Optional current user request to keep in scope while following the guide.",
                    required=False,
                )
            ],
        )
    ]


@app.get_prompt()
async def get_prompt(name: str, arguments: dict[str, str] | None) -> GetPromptResult:
    if name != _MEMORY_TOOL_GUIDE_PROMPT:
        raise ValueError(f"Unknown prompt: {name}")

    user_request = ""
    if isinstance(arguments, dict):
        raw_value = arguments.get("user_request", "")
        if isinstance(raw_value, str):
            user_request = raw_value.strip()

    return GetPromptResult(
        description="Optional guidance for assistants deciding when to recall or save memory.",
        messages=[
            PromptMessage(
                role="user",
                content=TextContent(
                    type="text",
                    text=_build_memory_tool_guide_text(user_request),
                ),
            )
        ],
    )


async def _maybe_preload_embedding_model() -> None:
    if config.PRELOAD_EMBEDDING_MODEL:
        # Optional preload to reduce first recall latency after the model is already cached.
        from src.engine.chroma_engine import chroma_enabled, _get_model
        if chroma_enabled():
            await asyncio.to_thread(_get_model)


async def run_stdio_server() -> None:
    await _maybe_preload_embedding_model()

    async with stdio_server() as (read_stream, write_stream):
        try:
            await app.run(read_stream, write_stream, app.create_initialization_options())
        finally:
            await close_connection()


class StreamableHTTPASGIApp:
    def __init__(self, session_manager: StreamableHTTPSessionManager):
        self._session_manager = session_manager

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        await self._session_manager.handle_request(scope, receive, send)


def create_streamable_http_app() -> Starlette:
    session_manager = StreamableHTTPSessionManager(
        app=app,
        json_response=False,
        stateless=False,
    )

    transport_app = StreamableHTTPASGIApp(session_manager)

    @asynccontextmanager
    async def lifespan(_: Starlette):
        try:
            await _maybe_preload_embedding_model()
            async with session_manager.run():
                yield
        finally:
            await close_connection()

    return Starlette(
        routes=[
            Route(
                config.MCP_HTTP_PATH,
                endpoint=transport_app,
                methods=["GET", "POST", "DELETE"],
            )
        ],
        lifespan=lifespan,
    )


def run_streamable_http_server() -> None:
    uvicorn.run(
        create_streamable_http_app(),
        host=config.MCP_HTTP_HOST,
        port=config.MCP_HTTP_PORT,
        log_level="info",
    )


def main() -> None:
    if config.MCP_TRANSPORT == "stdio":
        asyncio.run(run_stdio_server())
        return

    if config.MCP_TRANSPORT == "streamable-http":
        run_streamable_http_server()
        return

    raise ValueError(f"Unsupported MCP transport: {config.MCP_TRANSPORT}")


if __name__ == "__main__":
    main()
