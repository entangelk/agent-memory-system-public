"""
memory_recall 통합 테스트.
MongoDB가 localhost:27017에서 실행 중이어야 합니다.
"""
import json
from datetime import datetime, timedelta, UTC
import pytest
from src.engine.memory_engine import save_memory
from src.engine import sensitivity_engine
from src.tools.memory_recall import handle
from src.db import collections as col
from src.db.connection import get_db


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
    await db["topics"].delete_many({"slug": "recall-test-topic"})
    await db["memories"].delete_many({"category": "fact", "entities": {"$in": ["recall_test"]}})
    await db["digests"].delete_many({"period": "test-recall-stale"})
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
    await db["topics"].delete_many({"slug": "recall-test-topic"})
    await db["memories"].delete_many({"entities": {"$in": ["recall_test"]}})
    await db["digests"].delete_many({"period": "test-recall-stale"})


async def test_recall_returns_results():
    await save_memory(
        content="파이썬은 훌륭한 프로그래밍 언어입니다.",
        category="fact",
        importance=6,
        entities=["파이썬", "recall_test"],
    )

    result = await handle("memory_recall", {"query": "파이썬"})
    assert result is not None
    assert len(result) > 0
    assert "파이썬" in result[0].text or "Results" in result[0].text


async def test_recall_unknown_tool_returns_none():
    result = await handle("unknown_tool", {})
    assert result is None


async def test_recall_no_results():
    result = await handle("memory_recall", {"query": "존재하지않는쿼리XYZ_12345"})
    assert result is not None
    assert "No matching memories found." in result[0].text or "Results" in result[0].text


async def test_recall_supports_date_browsing_without_query(monkeypatch):
    recent_id = await save_memory(
        content="오늘 작업 recall_date_browse_recent",
        category="event",
        importance=6,
        entities=["recall_test"],
    )
    old_id = await save_memory(
        content="지난주 작업 recall_date_browse_old",
        category="event",
        importance=5,
        entities=["recall_test"],
    )
    await col.update_one(
        col.memories(),
        old_id,
        {"$set": {"created_at": datetime.now(UTC) - timedelta(days=10)}},
    )

    captured: dict = {}

    async def fake_search_memory_ids_with_scores(*, query: str | None = None, top_k: int, filters: dict | None = None):
        captured["query"] = query
        captured["top_k"] = top_k
        captured["filters"] = filters
        return [(str(recent_id), 0.9), (str(old_id), 0.5)]

    async def fake_search_topic_ids_with_scores(*, query: str, top_k: int, filters: dict | None = None):
        pytest.fail("search_topic_ids_with_scores should not be called when query is empty")

    monkeypatch.setattr(
        "src.tools.memory_recall.chroma_engine.search_memory_ids_with_scores",
        fake_search_memory_ids_with_scores,
    )
    monkeypatch.setattr(
        "src.tools.memory_recall.chroma_engine.search_topic_ids_with_scores",
        fake_search_topic_ids_with_scores,
    )

    result = await handle("memory_recall", {"time_range": "1d", "include_debug": True})
    assert result is not None
    payload = json.loads(result[0].text)
    result_ids = {row.get("id") for row in payload.get("results", [])}

    assert str(recent_id) in result_ids
    assert str(old_id) not in result_ids
    assert captured.get("query") is None
    assert isinstance(captured.get("filters"), dict)
    assert "created_at_ts" in captured["filters"]


async def test_recall_debug_echoes_trimmed_query():
    await save_memory(
        content="debug query echo recall_debug_query_case",
        category="fact",
        importance=6,
        entities=["recall_test"],
    )

    result = await handle("memory_recall", {"query": "  recall_debug_query_case  ", "include_debug": True})
    assert result is not None
    payload = json.loads(result[0].text)
    assert payload.get("query") == "recall_debug_query_case"


async def test_recall_debug_includes_source_metadata():
    await save_memory(
        content="recall source metadata case",
        category="fact",
        importance=6,
        entities=["recall_test"],
        source_agent="gpt-5.4",
        source_client="Codex",
    )

    result = await handle("memory_recall", {"query": "recall source metadata case", "include_debug": True})
    assert result is not None
    payload = json.loads(result[0].text)
    rows = payload.get("results", [])
    assert rows
    row = rows[0]
    assert row.get("source_agent") == "gpt-5.4"
    assert row.get("source_client") == "Codex"


async def test_recall_with_category_filter():
    await save_memory(
        content="좋아하는 음식은 김치찌개입니다.",
        category="preference",
        importance=8,
        entities=["김치찌개", "recall_test"],
    )

    result = await handle("memory_recall", {"query": "recall_test", "category": "preference"})
    assert result is not None
    text = result[0].text
    assert "Results" in text or "No matching memories found." in text


async def test_recall_merges_memory_vector_results_when_centroid_empty(monkeypatch):
    oid = await save_memory(
        content="memory vector 병합 테스트 문장 recall_vector_merge_case",
        category="fact",
        importance=7,
        entities=["recall_test"],
    )

    async def fake_search_topic_ids_with_scores(*, query: str, top_k: int, filters: dict | None = None):
        return []

    async def fake_search_memory_ids_with_scores(*, query: str | None = None, top_k: int, filters: dict | None = None):
        return [(str(oid), 0.85)]

    monkeypatch.setattr(
        "src.tools.memory_recall.chroma_engine.search_topic_ids_with_scores",
        fake_search_topic_ids_with_scores,
    )
    monkeypatch.setattr(
        "src.tools.memory_recall.chroma_engine.search_memory_ids_with_scores",
        fake_search_memory_ids_with_scores,
    )

    # query 문자열이 content에 직접 없더라도 memory vector id가 전달되면 조회되어야 함
    result = await handle("memory_recall", {"query": "의미기반질의_vector_only"})
    assert result is not None
    assert "recall_vector_merge_case" in result[0].text


async def test_recall_uses_centroid_topic_ids_when_available(monkeypatch):
    topic_id = await col.insert_one(col.topics(), {
        "slug": "recall-test-topic",
        "title": "회상 테스트 토픽",
        "type": "centroid",
        "status": "active",
        "summary": "회상 테스트 요약",
        "sections": {},
        "linked_memory_ids": [],
        "linked_triple_ids": [],
        "category_id": None,
        "memory_count": 1,
        "auto_generated": True,
    })

    oid = await save_memory(
        content="벡터 검색 전용 문장",
        category="fact",
        importance=7,
        entities=["recall_test"],
    )
    await col.update_one(col.memories(), oid, {"$set": {"topic_id": topic_id}})

    async def fake_search_topic_ids_with_scores(*, query: str, top_k: int, filters: dict | None = None):
        return [(str(topic_id), 0.75)]

    monkeypatch.setattr(
        "src.tools.memory_recall.chroma_engine.search_topic_ids_with_scores",
        fake_search_topic_ids_with_scores,
    )

    # query 단어가 content에 직접 없더라도 topic_id 매칭이면 조회되어야 함
    result = await handle("memory_recall", {"query": "의미기반질의"})
    assert result is not None
    assert "Macro context" in result[0].text
    assert "회상 테스트 토픽" in result[0].text
    assert "벡터 검색 전용 문장" in result[0].text


async def test_recall_falls_back_to_text_search_on_chroma_error(monkeypatch):
    await save_memory(
        content="폴백 검색 키워드 alpha123",
        category="fact",
        importance=6,
        entities=["recall_test"],
    )

    async def broken_search(*, query: str, top_k: int, filters: dict | None = None):
        raise RuntimeError("chroma unavailable")

    monkeypatch.setattr(
        "src.tools.memory_recall.chroma_engine.search_topic_ids_with_scores",
        broken_search,
    )

    result = await handle("memory_recall", {"query": "alpha123"})
    assert result is not None
    assert "폴백 검색 키워드" in result[0].text


async def test_recall_returns_centroid_refresh_recommendation_when_stale(monkeypatch):
    topic_id = await col.insert_one(col.topics(), {
        "slug": "recall-test-topic",
        "title": "회상 테스트 토픽",
        "type": "centroid",
        "status": "active",
        "summary": "오래된 요약",
        "sections": {},
        "linked_memory_ids": [],
        "linked_triple_ids": [],
        "category_id": None,
        "memory_count": 1,
        "auto_generated": True,
        "centroid_version": 1,
        "centroid_updated_at": datetime.now(UTC) - timedelta(days=30),
    })

    memory_id = await save_memory(
        content="stale centroid 확인용 문장 beta_stale_key",
        category="fact",
        importance=7,
        entities=["recall_test"],
    )
    await col.update_one(col.memories(), memory_id, {"$set": {"topic_id": topic_id}})

    await col.insert_one(col.digests(), {
        "type": "weekly",
        "period": "test-recall-stale",
        "content": "새 digest",
        "topic_id": topic_id,
        "created_at": datetime.now(UTC),
        "updated_at": datetime.now(UTC),
    })

    async def fake_search_topic_ids_with_scores(*, query: str, top_k: int, filters: dict | None = None):
        return [(str(topic_id), 0.7)]

    monkeypatch.setattr(
        "src.tools.memory_recall.chroma_engine.search_topic_ids_with_scores",
        fake_search_topic_ids_with_scores,
    )

    result = await handle("memory_recall", {"query": "beta_stale_key", "include_debug": True})
    assert result is not None
    payload = json.loads(result[0].text)
    recommendations = payload.get("centroid_refresh_recommendations", [])
    assert any(row.get("topic_id") == str(topic_id) for row in recommendations)
    assert any(str(memory_id) == row.get("id") for row in payload.get("results", []))


async def test_recall_hides_sensitive_content_by_policy():
    await col.profiles().update_one(
        {"user_id": "primary"},
        {
            "$set": {
                "sensitivity_policy": sensitivity_engine.normalize_policy({
                    "hide_sensitive_on_recall": True,
                    "agent_instruction": "Hide sensitive details in default output.",
                }),
                "updated_at": datetime.now(UTC),
            },
        },
        upsert=True,
    )

    await save_memory(
        content="테스트 주민번호 민감 내용 recall_sensitive_case",
        category="fact",
        importance=8,
        sensitivity="high",
        entities=["recall_test"],
    )
    await save_memory(
        content="일반 내용 recall_sensitive_case normal",
        category="fact",
        importance=6,
        sensitivity="normal",
        entities=["recall_test"],
    )

    result = await handle("memory_recall", {"query": "recall_sensitive_case", "include_debug": True})
    assert result is not None
    payload = json.loads(result[0].text)
    assert payload.get("sensitivity_policy", {}).get("applied_redaction") is True
    assert any(row.get("sensitivity") == "high" and row.get("is_redacted") is True for row in payload.get("results", []))


async def test_recall_include_sensitive_true_shows_sensitive_content():
    await col.profiles().update_one(
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

    await save_memory(
        content="고민감 정보 recall_sensitive_expand_case",
        category="fact",
        importance=8,
        sensitivity="high",
        entities=["recall_test"],
    )

    result = await handle("memory_recall", {
        "query": "recall_sensitive_expand_case",
        "include_debug": True,
        "include_sensitive": True,
    })
    assert result is not None
    payload = json.loads(result[0].text)
    rows = payload.get("results", [])
    assert rows
    assert any(row.get("sensitivity") == "high" and row.get("is_redacted") is False for row in rows)
    assert any("recall_sensitive_expand_case" in row.get("content", "") for row in rows)


async def test_recall_debug_includes_similarity_and_combined_score(monkeypatch):
    oid = await save_memory(
        content="similarity debug 출력 테스트 문장",
        category="fact",
        importance=5,
        entities=["recall_test"],
    )

    async def fake_search_topic_ids_with_scores(*, query: str, top_k: int, filters: dict | None = None):
        return []

    async def fake_search_memory_ids_with_scores(*, query: str | None = None, top_k: int, filters: dict | None = None):
        return [(str(oid), 0.82)]

    monkeypatch.setattr(
        "src.tools.memory_recall.chroma_engine.search_topic_ids_with_scores",
        fake_search_topic_ids_with_scores,
    )
    monkeypatch.setattr(
        "src.tools.memory_recall.chroma_engine.search_memory_ids_with_scores",
        fake_search_memory_ids_with_scores,
    )

    result = await handle("memory_recall", {"query": "similarity debug", "include_debug": True})
    assert result is not None
    payload = json.loads(result[0].text)
    rows = payload.get("results", [])
    assert rows
    row = rows[0]
    assert "similarity" in row
    assert "memory_score" in row
    assert "combined_score" in row
    assert row["similarity"] == 0.82
    assert row["combined_score"] > row["memory_score"]
