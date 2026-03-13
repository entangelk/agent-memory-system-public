"""
session_digest + memory_resources 통합 테스트.
MongoDB가 localhost:27017에서 실행 중이어야 합니다.
"""
import json
from datetime import datetime, UTC
import pytest
from src.db.connection import get_db
from src.db import collections as col
from src.engine import sensitivity_engine
from src.tools.session_digest import handle as digest_handle
from src.resources import memory_resources

TEST_SESSION = "session_digest_it_session"
CONTENT_PREFIX = "session_digest_it_"


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
    await db["memories"].delete_many({"content": {"$regex": f"^{CONTENT_PREFIX}"}})
    await db["pending_memories"].delete_many({"source_session": TEST_SESSION})
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
    await db["memories"].delete_many({"content": {"$regex": f"^{CONTENT_PREFIX}"}})
    await db["pending_memories"].delete_many({"source_session": TEST_SESSION})


async def test_session_digest_unknown_tool_returns_none():
    result = await digest_handle("unknown_tool", {})
    assert result is None


async def test_session_digest_updates_stats_resource_counts():
    before_total = await col.memories().count_documents({})
    before_pending = await col.pending_memories().count_documents({"status": "pending"})

    result = await digest_handle("session_digest", {
        "session_id": TEST_SESSION,
        "candidates": [
            {
                "content": f"{CONTENT_PREFIX}saved",
                "category": "fact",
                "importance": 10,
                "entities": ["session_digest_it_entity"],
            },
            {
                "content": f"{CONTENT_PREFIX}pending",
                "category": "event",
                "importance": 8,
                "entities": ["session_digest_it_entity"],
            },
            {
                "content": f"{CONTENT_PREFIX}ignored",
                "category": "emotion",
                "importance": 2,
                "entities": ["session_digest_it_entity"],
            },
        ],
    })

    assert result is not None
    summary = result[0].text
    assert "saved immediately: 1" in summary
    assert "pending approval: 1" in summary
    assert "ignored: 1" in summary

    saved = await col.memories().find_one({"content": f"{CONTENT_PREFIX}saved"})
    assert saved is not None

    pending = await col.pending_memories().find_one({
        "content": f"{CONTENT_PREFIX}pending",
        "source_session": TEST_SESSION,
        "status": "pending",
    })
    assert pending is not None

    stats = json.loads(await memory_resources._stats())
    assert stats["total_memories"] == before_total + 1
    assert stats["pending_approval"] == before_pending + 1


async def test_session_digest_saved_memory_appears_in_recent_resource():
    content = f"{CONTENT_PREFIX}recent"
    await digest_handle("session_digest", {
        "session_id": TEST_SESSION,
        "candidates": [
            {"content": content, "category": "fact", "importance": 10, "entities": []},
        ],
    })

    recent = json.loads(await memory_resources._recent())
    assert any(item["content"] == content for item in recent)


async def test_recent_resource_redacts_high_sensitivity_when_policy_enabled():
    db = get_db()
    await db["profiles"].update_one(
        {"user_id": "primary"},
        {"$set": {"sensitivity_policy": sensitivity_engine.normalize_policy({"hide_sensitive_on_recall": True})}},
        upsert=True,
    )

    await digest_handle("session_digest", {
        "session_id": TEST_SESSION,
        "candidates": [
            {"content": f"{CONTENT_PREFIX}sensitive", "category": "fact", "importance": 10, "sensitivity": "high", "entities": []},
        ],
    })
    saved = await col.memories().find_one({"content": f"{CONTENT_PREFIX}sensitive"})
    assert saved is not None

    recent = json.loads(await memory_resources._recent())
    target = next((item for item in recent if item.get("id") == str(saved["_id"])), None)
    assert target is not None
    assert target.get("sensitivity") == "high"
    assert target.get("is_redacted") is True


async def test_memory_resources_profile_and_compaction_status_return_json():
    profile = json.loads(await memory_resources._profile())
    assert isinstance(profile, dict)
    if "message" not in profile:
        assert profile.get("user_id") == "primary"

    status = json.loads(await memory_resources._compaction_status())
    for level in ["L1_daily", "L2_weekly", "L3_monthly", "L4_yearly"]:
        assert level in status
        assert "pending_count" in status[level]
        assert "threshold" in status[level]
        assert "ready" in status[level]
