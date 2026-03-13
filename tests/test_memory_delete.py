"""
memory_delete 통합 테스트.
MongoDB가 localhost:27017에서 실행 중이어야 합니다.
"""
import json
import pytest
from bson import ObjectId
from src.db.connection import get_db
from src.db import collections as col
from src.engine.memory_engine import save_memory
from src.tools.memory_delete import handle

TEST_CONTEXT = "memory_delete_test_ctx"


@pytest.fixture(autouse=True)
async def cleanup():
    db = get_db()
    await db["memories"].delete_many({"context": TEST_CONTEXT})
    await db["topics"].delete_many({"slug": "memory-delete-topic"})
    yield
    await db["memories"].delete_many({"context": TEST_CONTEXT})
    await db["topics"].delete_many({"slug": "memory-delete-topic"})


async def test_memory_delete_unknown_tool_returns_none():
    result = await handle("unknown_tool", {})
    assert result is None


async def test_memory_delete_rejects_invalid_id():
    result = await handle("memory_delete", {"memory_id": "not-an-id"})
    assert result is not None
    assert "Invalid id" in result[0].text


async def test_memory_delete_rejects_not_found():
    result = await handle("memory_delete", {"memory_id": str(ObjectId())})
    assert result is not None
    assert "Memory not found." in result[0].text


async def test_memory_delete_removes_document():
    oid = await save_memory(
        content="삭제 대상 memory",
        category="fact",
        importance=4,
        context=TEST_CONTEXT,
        entities=["memory_delete_test"],
    )
    result = await handle("memory_delete", {"memory_id": str(oid)})
    assert result is not None
    payload = json.loads(result[0].text)
    assert payload["status"] == "deleted"

    doc = await col.find_by_id(col.memories(), oid)
    assert doc is None


async def test_memory_delete_updates_topic_memory_count():
    topic_id = await col.insert_one(col.topics(), {
        "slug": "memory-delete-topic",
        "title": "Memory Delete Topic",
        "type": "centroid",
        "status": "active",
        "summary": "",
        "sections": {},
        "linked_memory_ids": [],
        "linked_triple_ids": [],
        "category_id": None,
        "memory_count": 2,
        "auto_generated": True,
    })
    oid1 = await col.insert_one(col.memories(), {
        "content": "topic linked 1",
        "category": "fact",
        "importance": 5,
        "entities": [],
        "context": TEST_CONTEXT,
        "compressed_from": [],
        "recall_count": 0,
        "memory_tier": "short_term",
        "topic_id": topic_id,
    })
    await col.insert_one(col.memories(), {
        "content": "topic linked 2",
        "category": "fact",
        "importance": 5,
        "entities": [],
        "context": TEST_CONTEXT,
        "compressed_from": [],
        "recall_count": 0,
        "memory_tier": "short_term",
        "topic_id": topic_id,
    })

    result = await handle("memory_delete", {"memory_id": str(oid1)})
    assert result is not None
    payload = json.loads(result[0].text)
    assert payload["status"] == "deleted"
    assert payload["topic_memory_count"] == 1

    topic = await col.find_by_id(col.topics(), topic_id)
    assert topic is not None
    assert topic["memory_count"] == 1
