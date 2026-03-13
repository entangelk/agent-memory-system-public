"""
memory_update 통합 테스트.
MongoDB가 localhost:27017에서 실행 중이어야 합니다.
"""
import json
import pytest
from bson import ObjectId
from src.db.connection import get_db
from src.db import collections as col
from src.engine.memory_engine import save_memory
from src.tools.memory_update import handle

TEST_CONTEXT = "memory_update_test_ctx"


@pytest.fixture(autouse=True)
async def cleanup():
    db = get_db()
    await db["memories"].delete_many({"context": TEST_CONTEXT})
    await db["topics"].delete_many({"slug": "memory-update-residual-topic"})
    await db["topic_residual_mappings"].delete_many({})
    yield
    await db["memories"].delete_many({"context": TEST_CONTEXT})
    await db["topics"].delete_many({"slug": "memory-update-residual-topic"})
    await db["topic_residual_mappings"].delete_many({})


async def test_memory_update_unknown_tool_returns_none():
    result = await handle("unknown_tool", {})
    assert result is None


async def test_memory_update_rejects_invalid_id():
    result = await handle("memory_update", {"memory_id": "not-an-id", "content": "x"})
    assert result is not None
    assert "Invalid id" in result[0].text


async def test_memory_update_rejects_not_found():
    result = await handle("memory_update", {"memory_id": str(ObjectId()), "content": "x"})
    assert result is not None
    assert "Memory not found." in result[0].text


async def test_memory_update_requires_fields():
    oid = await save_memory(
        content="수정 전",
        category="fact",
        importance=5,
        context=TEST_CONTEXT,
        entities=["memory_update_test"],
    )
    result = await handle("memory_update", {"memory_id": str(oid)})
    assert result is not None
    assert "No updatable fields were provided." in result[0].text


async def test_memory_update_updates_memory_fields():
    oid = await save_memory(
        content="원본 내용",
        category="fact",
        importance=5,
        context=TEST_CONTEXT,
        entities=["memory_update_test"],
    )
    result = await handle("memory_update", {
        "memory_id": str(oid),
        "content": "수정된 내용",
        "importance": 9,
    })
    assert result is not None
    payload = json.loads(result[0].text)
    assert payload["status"] == "updated"
    assert "content" in payload["updated_fields"]
    assert "importance" in payload["updated_fields"]

    doc = await col.find_by_id(col.memories(), oid)
    assert doc is not None
    assert doc["content"] == "수정된 내용"
    assert doc["importance"] == 9


async def test_memory_update_sensitivity_field():
    oid = await save_memory(
        content="민감도 변경 전",
        category="fact",
        importance=5,
        context=TEST_CONTEXT,
        entities=["memory_update_test"],
    )
    result = await handle("memory_update", {
        "memory_id": str(oid),
        "sensitivity": "high",
    })
    assert result is not None
    payload = json.loads(result[0].text)
    assert payload["status"] == "updated"
    assert "sensitivity" in payload["updated_fields"]

    doc = await col.find_by_id(col.memories(), oid)
    assert doc is not None
    assert doc.get("sensitivity") == "high"


async def test_memory_update_entities_calls_topic_engine(monkeypatch):
    called = {"value": False}

    async def fake_maybe_create_topic(entities: list[str]):
        called["value"] = True
        return None

    monkeypatch.setattr(
        "src.tools.memory_update.topic_engine.maybe_create_topic",
        fake_maybe_create_topic,
    )

    oid = await save_memory(
        content="엔티티 업데이트 테스트",
        category="fact",
        importance=5,
        context=TEST_CONTEXT,
        entities=["before_entity"],
    )
    result = await handle("memory_update", {
        "memory_id": str(oid),
        "entities": ["after_entity"],
    })
    assert result is not None
    assert called["value"] is True


async def test_memory_update_residual_info_syncs_topic_mapping():
    topic_id = await col.insert_one(col.topics(), {
        "slug": "memory-update-residual-topic",
        "title": "Memory Update Residual Topic",
        "type": "centroid",
        "status": "active",
        "summary": "",
        "sections": {},
        "linked_memory_ids": [],
        "linked_triple_ids": [],
        "category_id": None,
        "memory_count": 1,
        "auto_generated": True,
    })
    memory_id = await col.insert_one(col.memories(), {
        "content": "residual update target",
        "category": "fact",
        "importance": 5,
        "entities": [],
        "context": TEST_CONTEXT,
        "compressed_from": [],
        "recall_count": 0,
        "memory_tier": "short_term",
        "topic_id": topic_id,
        "residual_info": {},
        "residual_slots": [],
    })

    update_result = await handle("memory_update", {
        "memory_id": str(memory_id),
        "residual_info": {
            "Place": "Pangyo",
            "Stack": ["FastAPI", "mongodb", "MongoDB"],
        },
    })
    assert update_result is not None
    update_payload = json.loads(update_result[0].text)
    assert update_payload["status"] == "updated"
    assert "residual_info" in update_payload["updated_fields"]

    doc = await col.find_by_id(col.memories(), memory_id)
    assert doc is not None
    assert doc.get("residual_info", {}).get("location") == "Pangyo"
    assert doc.get("residual_info", {}).get("stack") == ["FastAPI", "mongodb"]
    assert set(doc.get("residual_slots", [])) == {"location", "stack"}

    mapping_docs = await col.topic_residual_mappings().find(
        {"topic_id": topic_id, "memory_ids": memory_id},
        projection={"slot": 1, "value": 1},
    ).to_list(length=10)
    assert len(mapping_docs) == 3

    clear_result = await handle("memory_update", {
        "memory_id": str(memory_id),
        "residual_info": {},
    })
    assert clear_result is not None
    clear_payload = json.loads(clear_result[0].text)
    assert clear_payload["status"] == "updated"

    mapped_after_clear = await col.topic_residual_mappings().find(
        {"topic_id": topic_id, "memory_ids": memory_id},
    ).to_list(length=10)
    assert len(mapped_after_clear) == 0
