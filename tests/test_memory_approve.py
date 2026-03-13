"""
memory_approve 통합 테스트.
MongoDB가 localhost:27017에서 실행 중이어야 합니다.
"""
from datetime import datetime, UTC
import pytest
from bson import ObjectId
from src.db import collections as col
from src.db.connection import get_db
from src.engine import sensitivity_engine
from src.tools.memory_approve import handle

TEST_SESSION = "memory_approve_test_session"
TEST_ENTITY = "memory_approve_test_entity"


@pytest.fixture(autouse=True)
async def cleanup():
    """테스트 전후 테스트 데이터 정리."""
    db = get_db()
    await db["profiles"].update_one(
        {"user_id": "primary"},
        {
            "$set": {
                "sensitivity_policy": sensitivity_engine.normalize_policy(None),
                "updated_at": datetime.now(UTC),
            },
            "$setOnInsert": {
                "summary": "",
                "preferences": {},
                "communication_style": "",
                "last_consolidated": None,
                "created_at": datetime.now(UTC),
            },
        },
        upsert=True,
    )
    await db["pending_memories"].delete_many({"source_session": TEST_SESSION})
    await db["memories"].delete_many({"entities": {"$in": [TEST_ENTITY]}})
    await db["topics"].delete_many({"title": {"$regex": TEST_ENTITY}})
    yield
    await db["profiles"].update_one(
        {"user_id": "primary"},
        {
            "$set": {
                "sensitivity_policy": sensitivity_engine.normalize_policy(None),
                "updated_at": datetime.now(UTC),
            },
            "$setOnInsert": {
                "summary": "",
                "preferences": {},
                "communication_style": "",
                "last_consolidated": None,
                "created_at": datetime.now(UTC),
            },
        },
        upsert=True,
    )
    await db["pending_memories"].delete_many({"source_session": TEST_SESSION})
    await db["memories"].delete_many({"entities": {"$in": [TEST_ENTITY]}})
    await db["topics"].delete_many({"title": {"$regex": TEST_ENTITY}})


async def _create_pending(
    content: str,
    status: str = "pending",
    importance: int = 8,
    category: str = "fact",
    entities: list[str] | None = None,
) -> ObjectId:
    return await col.insert_one(
        col.pending_memories(),
        {
            "content": content,
            "importance": importance,
            "source_session": TEST_SESSION,
            "suggested_category": category,
            "suggested_entities": entities or [TEST_ENTITY],
            "status": status,
            "presented_at": None,
        },
    )


async def test_memory_approve_unknown_tool_returns_none():
    result = await handle("unknown_tool", {})
    assert result is None


async def test_memory_approve_list_updates_presented_at():
    pending_id = await _create_pending("목록 조회용 pending")

    result = await handle("memory_approve", {"action": "list"})
    assert result is not None
    assert str(pending_id) in result[0].text
    assert "목록 조회용 pending" in result[0].text

    doc = await col.find_by_id(col.pending_memories(), pending_id)
    assert doc is not None
    assert doc["presented_at"] is not None


async def test_memory_approve_approve_moves_to_memories():
    pending_id = await _create_pending("승인 대상 기억", importance=9, category="preference")

    result = await handle("memory_approve", {"action": "approve", "pending_id": str(pending_id)})
    assert result is not None
    assert "Approved and saved as memory" in result[0].text

    pending_doc = await col.find_by_id(col.pending_memories(), pending_id)
    assert pending_doc is not None
    assert pending_doc["status"] == "approved"

    saved = await col.memories().find_one({"content": "승인 대상 기억", "entities": {"$in": [TEST_ENTITY]}})
    assert saved is not None
    assert saved["category"] == "preference"
    assert saved["importance"] == 9


async def test_memory_approve_dismiss_sets_dismissed():
    pending_id = await _create_pending("거절 대상 기억")

    result = await handle("memory_approve", {"action": "dismiss", "pending_id": str(pending_id)})
    assert result is not None
    assert "Dismissed." in result[0].text

    pending_doc = await col.find_by_id(col.pending_memories(), pending_id)
    assert pending_doc is not None
    assert pending_doc["status"] == "dismissed"

    rejected = await col.memories().find_one({"content": "거절 대상 기억", "entities": {"$in": [TEST_ENTITY]}})
    assert rejected is None


async def test_memory_approve_rejects_invalid_id():
    result = await handle("memory_approve", {"action": "approve", "pending_id": "not-an-object-id"})
    assert result is not None
    assert "Invalid id" in result[0].text


async def test_memory_approve_list_redacts_high_sensitivity_by_policy():
    db = get_db()
    await db["profiles"].update_one(
        {"user_id": "primary"},
        {
            "$set": {
                "sensitivity_policy": sensitivity_engine.normalize_policy({
                    "hide_sensitive_on_recall": True,
                    "agent_instruction": "Hide high-sensitivity details by default.",
                }),
                "updated_at": datetime.now(UTC),
            },
        },
        upsert=True,
    )
    pending_id = await _create_pending("고민감 pending 내용", importance=9, category="fact")
    await col.update_one(col.pending_memories(), pending_id, {"$set": {"suggested_sensitivity": "high"}})

    hidden = await handle("memory_approve", {"action": "list"})
    assert hidden is not None
    assert "[Sensitive content hidden]" in hidden[0].text

    shown = await handle("memory_approve", {"action": "list", "include_sensitive": True})
    assert shown is not None
    assert "고민감 pending 내용" in shown[0].text
