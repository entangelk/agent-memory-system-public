"""
memory_summarize 통합 테스트.
MongoDB가 localhost:27017에서 실행 중이어야 합니다.
"""
from datetime import datetime, UTC
import pytest
from src.db.connection import get_db
from src.engine.memory_engine import save_memory
from src.engine import sensitivity_engine
from src.tools.memory_summarize import handle

TEST_CONTEXT = "memory_summarize_test_ctx"


@pytest.fixture(autouse=True)
async def cleanup():
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
    await db["memories"].delete_many({"context": TEST_CONTEXT})
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
    await db["memories"].delete_many({"context": TEST_CONTEXT})


async def test_memory_summarize_hides_high_sensitivity_by_default_policy():
    db = get_db()
    await db["profiles"].update_one(
        {"user_id": "primary"},
        {"$set": {"sensitivity_policy": sensitivity_engine.normalize_policy({"hide_sensitive_on_recall": True})}},
        upsert=True,
    )
    await save_memory(
        content="고민감 요약 테스트",
        category="fact",
        importance=8,
        sensitivity="high",
        context=TEST_CONTEXT,
    )

    result = await handle("memory_summarize", {"category": "fact", "limit": 10})
    assert result is not None
    assert "[Sensitive content hidden]" in result[0].text


async def test_memory_summarize_include_sensitive_true_shows_content():
    db = get_db()
    await db["profiles"].update_one(
        {"user_id": "primary"},
        {"$set": {"sensitivity_policy": sensitivity_engine.normalize_policy({"hide_sensitive_on_recall": True})}},
        upsert=True,
    )
    await save_memory(
        content="요약 원문 노출 테스트",
        category="fact",
        importance=8,
        sensitivity="high",
        context=TEST_CONTEXT,
    )

    result = await handle("memory_summarize", {"category": "fact", "limit": 10, "include_sensitive": True})
    assert result is not None
    assert "요약 원문 노출 테스트" in result[0].text
