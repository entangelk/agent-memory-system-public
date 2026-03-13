from contextlib import asynccontextmanager

import pytest

from src import config
from src import server


@asynccontextmanager
async def _fake_stdio_server():
    yield object(), object()


async def _fake_to_thread(func):
    return func()


async def test_main_skips_embedding_preload_when_disabled(monkeypatch):
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

    await server.main()

    assert events == ["run", "close"]


async def test_main_preloads_embedding_when_enabled(monkeypatch):
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

    await server.main()

    assert events == ["get_model", "run", "close"]
