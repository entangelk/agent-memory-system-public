import asyncio
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from src.config import MONGODB_URI, DB_NAME

_client: AsyncIOMotorClient | None = None
_client_loop: asyncio.AbstractEventLoop | None = None


def get_client() -> AsyncIOMotorClient:
    global _client, _client_loop
    loop = asyncio.get_running_loop()

    # pytest-asyncio may create a new event loop per test. Reusing a Motor client
    # bound to a previous loop can raise "Event loop is closed".
    if _client is not None and (_client_loop is None or _client_loop.is_closed() or _client_loop != loop):
        _client.close()
        _client = None
        _client_loop = None

    if _client is None:
        _client = AsyncIOMotorClient(MONGODB_URI)
        _client_loop = loop
    return _client


def get_db() -> AsyncIOMotorDatabase:
    return get_client()[DB_NAME]


async def close_connection() -> None:
    global _client, _client_loop
    if _client is not None:
        _client.close()
        _client = None
        _client_loop = None
