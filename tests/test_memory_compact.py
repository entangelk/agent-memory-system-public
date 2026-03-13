"""
memory_compact + compaction_engine 통합 테스트.
MongoDB가 localhost:27017에서 실행 중이어야 합니다.
"""
import json
import pytest
from datetime import datetime, timedelta, UTC
from src.db import collections as col
from src.db.connection import get_db
from src.tools.memory_compact import handle
from src.engine.compaction_engine import get_compaction_status, get_compaction_hint
from src.engine import sensitivity_engine


TEST_MARKER = "_compact_test"


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
    await db["memories"].delete_many({"context": TEST_MARKER})
    await db["digests"].delete_many({"_test_marker": TEST_MARKER})
    await db["topics"].delete_many({"slug": "compact-centroid-target-test"})
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
    await db["memories"].delete_many({"context": TEST_MARKER})
    await db["digests"].delete_many({"_test_marker": TEST_MARKER})
    await db["topics"].delete_many({"slug": "compact-centroid-target-test"})


async def test_compact_unknown_tool_returns_none():
    result = await handle("unknown_tool", {})
    assert result is None


async def test_compact_no_level_returns_status():
    """level 미지정 시 전체 상태 반환."""
    result = await handle("memory_compact", {})
    assert result is not None
    data = json.loads(result[0].text)
    assert "L1_daily" in data
    assert "L2_weekly" in data
    assert "threshold" in data["L1_daily"]
    assert "ready" in data["L1_daily"]


async def test_compact_with_level_no_sources():
    """대상 없을 수 있는 상태에서도 응답 구조가 안정적인지 확인."""
    result = await handle("memory_compact", {"level": "L1_daily"})
    assert result is not None
    data = json.loads(result[0].text)
    assert data["level"] == "L1_daily"
    assert "sources" in data
    assert isinstance(data["sources"], list)


async def test_compact_l1_returns_yesterday_memories():
    """어제 저장된 미컴팩팅 기억이 L1 sources로 반환되는지 확인."""
    now = datetime.now(UTC)
    yesterday = now.replace(hour=12, minute=0, second=0, microsecond=0) - timedelta(days=1)

    # 어제 날짜로 기억 3건 삽입
    inserted_contents = []
    for i in range(3):
        content = f"compact test memory {i}"
        inserted_contents.append(content)
        await col.insert_one(col.memories(), {
            "content": content,
            "category": "fact",
            "importance": 5,
            "entities": [],
            "context": TEST_MARKER,
            "created_at": yesterday,
            "compressed_from": [],
            "recall_count": 0,
            "memory_tier": "short_term",
        })

    result = await handle("memory_compact", {"level": "L1_daily"})
    data = json.loads(result[0].text)
    assert data["level"] == "L1_daily"
    assert data["digest_type"] == "daily"
    assert data["count"] >= 3
    assert len(data["sources"]) >= 3
    source_contents = {s.get("content") for s in data["sources"]}
    for content in inserted_contents:
        assert content in source_contents
    assert "instruction" in data


async def test_compact_includes_centroid_targets_for_topic_linked_memories():
    now = datetime.now(UTC)
    yesterday = now.replace(hour=12, minute=0, second=0, microsecond=0) - timedelta(days=1)

    topic_id = await col.insert_one(col.topics(), {
        "slug": "compact-centroid-target-test",
        "title": "Compact Centroid Target Test",
        "type": "centroid",
        "status": "active",
        "summary": "",
        "sections": {},
        "linked_memory_ids": [],
        "linked_triple_ids": [],
        "category_id": None,
        "memory_count": 0,
        "auto_generated": True,
    })

    for i in range(2):
        await col.insert_one(col.memories(), {
            "content": f"centroid target memory {i}",
            "category": "fact",
            "importance": 5,
            "entities": [],
            "context": TEST_MARKER,
            "created_at": yesterday,
            "compressed_from": [],
            "recall_count": 0,
            "memory_tier": "short_term",
            "topic_id": topic_id,
        })

    result = await handle("memory_compact", {"level": "L1_daily"})
    data = json.loads(result[0].text)
    targets = data.get("centroid_targets", [])

    topic_target = None
    for target in targets:
        if target.get("topic_id") == str(topic_id):
            topic_target = target
            break

    assert topic_target is not None
    assert topic_target["source_count"] >= 2


async def test_compaction_status_reflects_threshold():
    """미컴팩팅 건수가 임계치 이상이면 ready=True."""
    now = datetime.now(UTC)
    yesterday = now.replace(hour=12, minute=0, second=0, microsecond=0) - timedelta(days=1)

    for i in range(3):
        await col.insert_one(col.memories(), {
            "content": f"status test memory {i}",
            "category": "fact",
            "importance": 5,
            "entities": [],
            "context": TEST_MARKER,
            "created_at": yesterday,
            "compressed_from": [],
            "recall_count": 0,
            "memory_tier": "short_term",
        })

    status = await get_compaction_status()
    assert status["L1_daily"]["pending_count"] >= 3
    assert status["L1_daily"]["ready"] is True


async def test_compaction_hint_only_ready_levels():
    """get_compaction_hint는 ready인 레벨만 반환."""
    hint = await get_compaction_hint()
    for level, info in hint.items():
        assert info["ready"] is True


async def test_compact_redacts_high_sensitivity_sources_when_policy_enabled():
    db = get_db()
    await db["profiles"].update_one(
        {"user_id": "primary"},
        {
            "$set": {
                "sensitivity_policy": sensitivity_engine.normalize_policy({
                    "hide_sensitive_on_recall": True,
                }),
                "updated_at": datetime.now(UTC),
            },
        },
        upsert=True,
    )

    now = datetime.now(UTC)
    yesterday = now.replace(hour=12, minute=0, second=0, microsecond=0) - timedelta(days=1)
    await col.insert_one(col.memories(), {
        "content": "compact sensitive content",
        "category": "fact",
        "importance": 7,
        "sensitivity": "high",
        "entities": [],
        "context": TEST_MARKER,
        "created_at": yesterday,
        "compressed_from": [],
        "recall_count": 0,
        "memory_tier": "short_term",
    })

    hidden = await handle("memory_compact", {"level": "L1_daily"})
    assert hidden is not None
    hidden_payload = json.loads(hidden[0].text)
    assert hidden_payload.get("redacted_count", 0) >= 1
    assert any(src.get("is_redacted") is True for src in hidden_payload.get("sources", []))

    shown = await handle("memory_compact", {"level": "L1_daily", "include_sensitive": True})
    assert shown is not None
    shown_payload = json.loads(shown[0].text)
    assert any(src.get("content") == "compact sensitive content" for src in shown_payload.get("sources", []))
