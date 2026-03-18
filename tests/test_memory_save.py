"""
memory_save 통합 테스트.
MongoDB가 localhost:27017에서 실행 중이어야 합니다.
"""
import json
from datetime import datetime, UTC
import pytest
from bson import ObjectId
from src.engine.memory_engine import save_memory
from src.engine import sensitivity_engine
from src.db import collections as col
from src.db.connection import get_db
from src.tools.memory_save import handle as save_handle

TEST_CONTEXT = "test_memory_save_ctx"


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
    await db["memories"].delete_many({"context": TEST_CONTEXT})
    await db["topics"].delete_many({"title": {"$regex": "자동생성엔티티_테스트"}})
    await db["topics"].delete_many({"slug": "centroid-update-test"})
    await db["topics"].delete_many({"slug": "centroid-infer-test"})
    await db["topics"].delete_many({"slug": "residual-save-test"})
    await db["topics"].delete_many({"slug": {"$regex": "^tax-l"}})
    await db["digests"].delete_many({"period": {"$in": ["test-2026-02-24", "test-2026-02-25", "test-2026-W08", "test-2026-W09"]}})
    await db["topic_residual_mappings"].delete_many({})

    # topic 자동 생성 규칙이 없으면 테스트용으로 최소 규칙 삽입
    exists = await db["rules"].find_one({
        "rule_type": "topic_generation",
        "enabled": True,
    })
    if not exists:
        await db["rules"].insert_one({
            "rule_type": "topic_generation",
            "description": "test topic generation rule",
            "enabled": True,
            "conditions": {"min_memory_count": 3},
            "_test": True,
        })

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
    await db["topics"].delete_many({"title": {"$regex": "자동생성엔티티_테스트"}})
    await db["topics"].delete_many({"slug": "centroid-update-test"})
    await db["topics"].delete_many({"slug": "centroid-infer-test"})
    await db["topics"].delete_many({"slug": "residual-save-test"})
    await db["topics"].delete_many({"slug": {"$regex": "^tax-l"}})
    await db["digests"].delete_many({"period": {"$in": ["test-2026-02-24", "test-2026-02-25", "test-2026-W08", "test-2026-W09"]}})
    await db["topic_residual_mappings"].delete_many({})
    await db["rules"].delete_many({"_test": True})


async def test_save_memory_returns_objectid():
    oid = await save_memory(
        content="테스트 기억입니다.",
        category="fact",
        importance=5,
        context=TEST_CONTEXT,
        entities=["테스트"],
    )
    assert isinstance(oid, ObjectId)


async def test_saved_memory_in_db():
    oid = await save_memory(
        content="저장 확인용 기억",
        category="event",
        importance=7,
        entities=["확인"],
        context=TEST_CONTEXT,
    )
    doc = await col.find_by_id(col.memories(), oid)
    assert doc is not None
    assert doc["content"] == "저장 확인용 기억"
    assert doc["category"] == "event"
    assert doc["importance"] == 7
    assert "확인" in doc["entities"]
    assert doc["sensitivity"] == "normal"
    assert doc["recall_count"] == 0
    assert doc["memory_tier"] == "short_term"


async def test_save_memory_upserts_chroma_memory_vector_with_created_at_metadata(monkeypatch):
    captured: dict = {}

    async def fake_upsert_memory_vector(
        *,
        memory_id: str,
        content: str,
        category: str = "",
        importance: int = 0,
        topic_id: str = "",
        metadata: dict | None = None,
    ):
        captured["memory_id"] = memory_id
        captured["content"] = content
        captured["category"] = category
        captured["importance"] = importance
        captured["topic_id"] = topic_id
        captured["metadata"] = metadata or {}
        return f"memory:{memory_id}"

    monkeypatch.setattr("src.engine.memory_engine.chroma_engine.chroma_enabled", lambda: True)
    monkeypatch.setattr(
        "src.engine.memory_engine.chroma_engine.upsert_memory_vector",
        fake_upsert_memory_vector,
    )

    oid = await save_memory(
        content="chroma upsert metadata test",
        category="fact",
        importance=6,
        context=TEST_CONTEXT,
        entities=[],
    )

    assert captured.get("memory_id") == str(oid)
    assert captured.get("category") == "fact"
    assert captured.get("importance") == 6
    metadata = captured.get("metadata", {})
    assert metadata.get("created_at_date")
    assert isinstance(metadata.get("created_at_ts"), int)
    assert metadata.get("created_at_ts", 0) > 0


async def test_saved_memory_stores_explicit_sensitivity():
    oid = await save_memory(
        content="민감도 저장 확인",
        category="fact",
        importance=6,
        sensitivity="high",
        context=TEST_CONTEXT,
    )
    doc = await col.find_by_id(col.memories(), oid)
    assert doc is not None
    assert doc["sensitivity"] == "high"


async def test_memory_save_tool_defaults_to_normal_sensitivity():
    result = await save_handle("memory_save", {
        "content": "tool default sensitivity",
        "category": "fact",
        "importance": 6,
        "context": TEST_CONTEXT,
    })
    assert result is not None
    payload = json.loads(result[0].text)
    assert payload.get("sensitivity") == "normal"

    doc = await col.memories().find_one({"_id": ObjectId(payload["id"])})
    assert doc is not None
    assert doc.get("sensitivity") == "normal"


async def test_memory_save_does_not_infer_by_keyword():
    result = await save_handle("memory_save", {
        "content": "내 주민번호는 테스트 데이터다.",
        "category": "fact",
        "importance": 6,
        "context": TEST_CONTEXT,
    })
    assert result is not None
    payload = json.loads(result[0].text)
    assert payload.get("sensitivity") == "normal"


async def test_memory_save_tool_stores_source_metadata():
    result = await save_handle("memory_save", {
        "content": "source metadata save case",
        "category": "fact",
        "importance": 7,
        "context": TEST_CONTEXT,
        "source_agent": " gpt-5.4 ",
        "source_client": " Codex ",
    })
    assert result is not None
    payload = json.loads(result[0].text)
    assert payload.get("source_agent") == "gpt-5.4"
    assert payload.get("source_client") == "Codex"

    doc = await col.memories().find_one({"_id": ObjectId(payload["id"])})
    assert doc is not None
    assert doc.get("source_agent") == "gpt-5.4"
    assert doc.get("source_client") == "Codex"


async def test_topic_auto_created_after_3_memories():
    """같은 엔티티를 공유하는 기억 3개 저장 시 topic 자동 생성."""
    entities = ["자동생성엔티티_테스트"]

    for i in range(3):
        await save_memory(
            content=f"자동 topic 테스트 기억 {i}",
            category="fact",
            importance=5,
            context=TEST_CONTEXT,
            entities=entities,
        )

    topic = await col.topics().find_one({"title": {"$regex": "자동생성엔티티"}})
    assert topic is not None, "기억 3개 후 topic이 자동 생성되어야 합니다."
    assert topic["auto_generated"] is True


async def test_save_memory_syncs_topic_centroid_when_topic_created(monkeypatch):
    called_topic_ids: list[str] = []

    async def fake_upsert_topic_centroid(*, topic_id: str, title: str, summary: str, memory_count: int = 0, metadata: dict | None = None):
        called_topic_ids.append(topic_id)
        return f"topic:{topic_id}"

    monkeypatch.setattr(
        "src.engine.topic_engine.chroma_engine.upsert_topic_centroid",
        fake_upsert_topic_centroid,
    )

    entities = ["자동생성엔티티_테스트"]
    for i in range(3):
        await save_memory(
            content=f"centroid sync 테스트 기억 {i}",
            category="fact",
            importance=5,
            context=TEST_CONTEXT,
            entities=entities,
        )

    assert len(called_topic_ids) >= 1


async def test_memory_save_updates_centroid_summary_via_tool(monkeypatch):
    async def fake_upsert_topic_centroid(*, topic_id: str, title: str, summary: str, memory_count: int = 0, metadata: dict | None = None):
        return f"topic:{topic_id}"

    monkeypatch.setattr(
        "src.engine.topic_engine.chroma_engine.upsert_topic_centroid",
        fake_upsert_topic_centroid,
    )

    topic_id = await col.insert_one(col.topics(), {
        "slug": "centroid-update-test",
        "title": "Centroid Update Test",
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

    result = await save_handle("memory_save", {
        "content": "컴팩팅 요약 저장 테스트",
        "category": "digest",
        "importance": 6,
        "context": TEST_CONTEXT,
        "centroid_topic_id": str(topic_id),
        "centroid_summary": "갱신된 centroid summary",
    })

    assert result is not None
    payload = json.loads(result[0].text)
    assert payload["status"] == "saved"
    assert payload.get("centroid_updated") is True

    topic = await col.find_by_id(col.topics(), topic_id)
    assert topic is not None
    assert topic["summary"] == "갱신된 centroid summary"


async def test_memory_save_digest_updates_centroid_version_and_source_ids(monkeypatch):
    calls: list[dict] = []

    async def fake_upsert_topic_centroid(*, topic_id: str, title: str, summary: str, memory_count: int = 0, metadata: dict | None = None):
        calls.append({
            "topic_id": topic_id,
            "summary": summary,
            "metadata": metadata or {},
        })
        return f"topic:{topic_id}"

    monkeypatch.setattr(
        "src.engine.topic_engine.chroma_engine.upsert_topic_centroid",
        fake_upsert_topic_centroid,
    )

    topic_id = await col.insert_one(col.topics(), {
        "slug": "centroid-update-test",
        "title": "Centroid Metadata Test",
        "type": "centroid",
        "status": "active",
        "summary": "old summary",
        "sections": {},
        "linked_memory_ids": [],
        "linked_triple_ids": [],
        "category_id": None,
        "memory_count": 1,
        "auto_generated": True,
        "centroid_version": 2,
        "centroid_source_digest_ids": [],
    })

    result = await save_handle("memory_save", {
        "content": "centroid metadata refresh",
        "category": "digest",
        "importance": 7,
        "context": TEST_CONTEXT,
        "digest_type": "daily",
        "digest_period": "test-2026-02-25",
        "centroid_topic_id": str(topic_id),
        "centroid_summary": "updated summary by digest",
    })
    assert result is not None
    payload = json.loads(result[0].text)
    assert payload.get("status") == "saved"
    assert payload.get("centroid_updated") is True
    assert payload.get("digest_record_id")

    topic = await col.find_by_id(col.topics(), topic_id)
    assert topic is not None
    assert topic.get("summary") == "updated summary by digest"
    assert topic.get("centroid_version") == 3
    assert topic.get("centroid_source_digest_ids") == [payload["digest_record_id"]]
    assert topic.get("centroid_updated_at") is not None

    assert calls
    assert calls[-1]["topic_id"] == str(topic_id)
    assert calls[-1]["metadata"].get("centroid_version") == 3
    assert calls[-1]["metadata"].get("centroid_updated_at")


async def test_memory_save_digest_marks_compacted_sources():
    source_id = await col.insert_one(col.memories(), {
        "content": "compaction source",
        "category": "fact",
        "importance": 5,
        "entities": [],
        "context": TEST_CONTEXT,
        "compressed_from": [],
        "recall_count": 0,
        "memory_tier": "short_term",
    })

    result = await save_handle("memory_save", {
        "content": "digest saved",
        "category": "digest",
        "importance": 6,
        "context": TEST_CONTEXT,
        "compacted_source_ids": [str(source_id)],
    })
    assert result is not None
    payload = json.loads(result[0].text)
    assert payload["status"] == "saved"
    assert payload.get("compacted_sources_updated", 0) >= 1

    source_doc = await col.find_by_id(col.memories(), source_id)
    assert source_doc is not None
    assert len(source_doc.get("compressed_from", [])) >= 1


async def test_memory_save_infers_centroid_topic_from_compacted_sources(monkeypatch):
    async def fake_upsert_topic_centroid(*, topic_id: str, title: str, summary: str, memory_count: int = 0, metadata: dict | None = None):
        return f"topic:{topic_id}"

    monkeypatch.setattr(
        "src.engine.topic_engine.chroma_engine.upsert_topic_centroid",
        fake_upsert_topic_centroid,
    )

    topic_id = await col.insert_one(col.topics(), {
        "slug": "centroid-infer-test",
        "title": "Centroid Infer Test",
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
    source_id = await col.insert_one(col.memories(), {
        "content": "topic-linked source memory",
        "category": "fact",
        "importance": 5,
        "entities": [],
        "context": TEST_CONTEXT,
        "compressed_from": [],
        "recall_count": 0,
        "memory_tier": "short_term",
        "topic_id": topic_id,
    })

    result = await save_handle("memory_save", {
        "content": "digest with inferred centroid",
        "category": "digest",
        "importance": 6,
        "context": TEST_CONTEXT,
        "compacted_source_ids": [str(source_id)],
        "centroid_summary": "inferred summary update",
    })
    assert result is not None
    payload = json.loads(result[0].text)
    assert payload["status"] == "saved"
    assert payload.get("centroid_topic_id_inferred") == str(topic_id)
    assert payload.get("centroid_updated") is True

    topic_doc = await col.find_by_id(col.topics(), topic_id)
    assert topic_doc is not None
    assert topic_doc["summary"] == "inferred summary update"


async def test_memory_save_digest_upserts_digest_record():
    result = await save_handle("memory_save", {
        "content": "daily digest content",
        "category": "digest",
        "importance": 6,
        "context": TEST_CONTEXT,
        "digest_type": "daily",
        "digest_period": "test-2026-02-24",
    })
    assert result is not None
    payload = json.loads(result[0].text)
    assert payload.get("digest_record_upserted") is True
    assert payload.get("digest_record_id")

    digest_doc = await col.digests().find_one({"type": "daily", "period": "test-2026-02-24"})
    assert digest_doc is not None
    assert digest_doc["content"] == "daily digest content"


async def test_memory_save_digest_marks_compacted_source_digests():
    source_digest_id = await col.insert_one(col.digests(), {
        "type": "daily",
        "period": "test-2026-W08",
        "content": "source digest",
    })

    result = await save_handle("memory_save", {
        "content": "weekly digest",
        "category": "digest",
        "importance": 7,
        "context": TEST_CONTEXT,
        "compacted_source_ids": [str(source_digest_id)],
        "digest_type": "weekly",
        "digest_period": "test-2026-02-24",
    })
    assert result is not None
    payload = json.loads(result[0].text)
    assert payload.get("compacted_digests_updated", 0) >= 1

    source_digest_doc = await col.find_by_id(col.digests(), source_digest_id)
    assert source_digest_doc is not None
    assert source_digest_doc.get("compacted_to") is not None


async def test_memory_save_digest_creates_topic_path_and_selects_layer_topic():
    result = await save_handle("memory_save", {
        "content": "weekly digest with topic path",
        "category": "digest",
        "importance": 7,
        "context": TEST_CONTEXT,
        "digest_type": "weekly",
        "digest_period": "test-2026-W09",
        "topic_path": ["프로젝트A 보안점검", "프로젝트A 보안", "보안개발", "개발/운영"],
        "topic_aliases": ["프로젝트A 점검"],
    })
    assert result is not None
    payload = json.loads(result[0].text)
    assert payload.get("digest_record_upserted") is True
    assert payload.get("digest_topic_id")
    nodes = payload.get("topic_path_nodes", [])
    assert len(nodes) == 4
    assert nodes[0]["level"] == 1
    assert nodes[1]["level"] == 2

    digest_doc = await col.digests().find_one({"type": "weekly", "period": "test-2026-W09"})
    assert digest_doc is not None
    assert str(digest_doc.get("topic_id")) == payload["digest_topic_id"]

    level1 = await col.find_by_id(col.topics(), ObjectId(nodes[0]["topic_id"]))
    level2 = await col.find_by_id(col.topics(), ObjectId(nodes[1]["topic_id"]))
    assert level1 is not None
    assert level2 is not None
    assert level1.get("level") == 1
    assert level2.get("level") == 2
    assert level2.get("parent_topic_id") == level1["_id"]
    assert "프로젝트A 점검" in level1.get("aliases", [])


async def test_memory_save_residual_info_normalized_and_topic_mapping_synced(monkeypatch):
    async def fake_upsert_topic_centroid(*, topic_id: str, title: str, summary: str, memory_count: int = 0, metadata: dict | None = None):
        return f"topic:{topic_id}"

    monkeypatch.setattr(
        "src.engine.topic_engine.chroma_engine.upsert_topic_centroid",
        fake_upsert_topic_centroid,
    )

    entities = ["residual save test"]
    memory_ids: list[ObjectId] = []
    for i in range(3):
        result = await save_handle("memory_save", {
            "content": f"residual 저장 테스트 {i}",
            "category": "fact",
            "importance": 6,
            "context": TEST_CONTEXT,
            "entities": entities,
            "residual_info": {
                "Loc": "Gangnam",
                "framework": ["FastAPI", "fastapi"],
                "why": "latency",
            },
        })
        assert result is not None
        memory_ids.append(ObjectId(json.loads(result[0].text)["id"]))

    docs = await col.memories().find(
        {"_id": {"$in": memory_ids}},
        projection={"topic_id": 1, "residual_info": 1, "residual_slots": 1},
    ).to_list(length=10)
    assert len(docs) == 3
    for doc in docs:
        assert doc.get("topic_id") is not None
        assert doc.get("residual_info", {}).get("location") == "Gangnam"
        assert doc.get("residual_info", {}).get("framework") == "FastAPI"
        assert doc.get("residual_info", {}).get("reason") == "latency"
        assert set(doc.get("residual_slots", [])) == {"framework", "location", "reason"}

    topic = await col.topics().find_one({"slug": "residual-save-test"})
    assert topic is not None

    mapping_location = await col.topic_residual_mappings().find_one({
        "topic_id": topic["_id"],
        "slot": "location",
        "value": "gangnam",
    })
    assert mapping_location is not None
    assert len(mapping_location.get("memory_ids", [])) == 3

    mapping_framework = await col.topic_residual_mappings().find_one({
        "topic_id": topic["_id"],
        "slot": "framework",
        "value": "fastapi",
    })
    assert mapping_framework is not None
    assert len(mapping_framework.get("memory_ids", [])) == 3
