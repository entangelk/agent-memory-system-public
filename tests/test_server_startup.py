from contextlib import asynccontextmanager

from src import config
from src import server


@asynccontextmanager
async def _fake_stdio_server():
    yield object(), object()


async def _fake_to_thread(func):
    return func()


async def test_run_stdio_server_skips_embedding_preload_when_disabled(monkeypatch):
    events: list[str] = []

    async def fake_run(read_stream, write_stream, init_options):
        events.append("run")

    async def fake_close_connection():
        events.append("close")

    def fake_get_model():
        events.append("get_model")
        return object()

    monkeypatch.setattr(config, "PRELOAD_EMBEDDING_MODEL", False)
    monkeypatch.setattr(server, "stdio_server", _fake_stdio_server)
    monkeypatch.setattr(server.asyncio, "to_thread", _fake_to_thread)
    monkeypatch.setattr(server.app, "run", fake_run)
    monkeypatch.setattr(server, "close_connection", fake_close_connection)
    monkeypatch.setattr("src.engine.chroma_engine.chroma_enabled", lambda: True)
    monkeypatch.setattr("src.engine.chroma_engine._get_model", fake_get_model)

    await server.run_stdio_server()

    assert events == ["run", "close"]


async def test_run_stdio_server_preloads_embedding_when_enabled(monkeypatch):
    events: list[str] = []

    async def fake_run(read_stream, write_stream, init_options):
        events.append("run")

    async def fake_close_connection():
        events.append("close")

    def fake_get_model():
        events.append("get_model")
        return object()

    monkeypatch.setattr(config, "PRELOAD_EMBEDDING_MODEL", True)
    monkeypatch.setattr(server, "stdio_server", _fake_stdio_server)
    monkeypatch.setattr(server.asyncio, "to_thread", _fake_to_thread)
    monkeypatch.setattr(server.app, "run", fake_run)
    monkeypatch.setattr(server, "close_connection", fake_close_connection)
    monkeypatch.setattr("src.engine.chroma_engine.chroma_enabled", lambda: True)
    monkeypatch.setattr("src.engine.chroma_engine._get_model", fake_get_model)

    await server.run_stdio_server()

    assert events == ["get_model", "run", "close"]


def test_main_dispatches_to_stdio_server(monkeypatch):
    events: list[str] = []

    async def fake_run_stdio_server():
        events.append("stdio")

    def fake_asyncio_run(coro):
        events.append("asyncio")
        import asyncio
        asyncio.get_event_loop_policy().new_event_loop().run_until_complete(coro)

    monkeypatch.setattr(config, "MCP_TRANSPORT", "stdio")
    monkeypatch.setattr(server, "run_stdio_server", fake_run_stdio_server)
    monkeypatch.setattr(server.asyncio, "run", fake_asyncio_run)

    server.main()

    assert events == ["asyncio", "stdio"]


def test_main_dispatches_to_streamable_http_server(monkeypatch):
    events: list[str] = []

    def fake_run_streamable_http_server():
        events.append("http")

    monkeypatch.setattr(config, "MCP_TRANSPORT", "streamable-http")
    monkeypatch.setattr(server, "run_streamable_http_server", fake_run_streamable_http_server)

    server.main()

    assert events == ["http"]
