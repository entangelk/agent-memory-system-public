"""
topic_lookup 통합 테스트.
"""
import json
import pytest
from bson import ObjectId
from src.db.connection import get_db
from src.tools.memory_save import handle as save_handle
from src.tools.topic_lookup import handle as topic_lookup_handle

TEST_CONTEXT = "test_topic_lookup_ctx"


@pytest.fixture(autouse=True)
async def cleanup():
    db = get_db()
    await db["memories"].delete_many({"context": TEST_CONTEXT})
    await db["digests"].delete_many({"period": {"$in": ["test-topic-2026-W09"]}})
    await db["topics"].delete_many({"slug": {"$regex": "^tax-l"}})
    yield
    await db["memories"].delete_many({"context": TEST_CONTEXT})
    await db["digests"].delete_many({"period": {"$in": ["test-topic-2026-W09"]}})
    await db["topics"].delete_many({"slug": {"$regex": "^tax-l"}})


async def test_topic_lookup_search_and_detail_with_children():
    save_result = await save_handle("memory_save", {
        "content": "topic lookup seed digest",
        "category": "digest",
        "importance": 7,
        "context": TEST_CONTEXT,
        "digest_type": "weekly",
        "digest_period": "test-topic-2026-W09",
        "topic_path": ["보안점검 작업", "보안프로젝트", "보안개발"],
        "topic_aliases": ["보안 점검"],
    })
    assert save_result is not None
    save_payload = json.loads(save_result[0].text)
    first_topic_id = save_payload["topic_path_nodes"][0]["topic_id"]

    search_result = await topic_lookup_handle("topic_lookup", {
        "query": "보안 점검",
        "top_k": 5,
    })
    assert search_result is not None
    search_payload = json.loads(search_result[0].text)
    assert search_payload.get("status") == "ok"
    assert search_payload.get("count", 0) >= 1
    ids = {row["id"] for row in search_payload.get("topics", [])}
    assert first_topic_id in ids

    detail_result = await topic_lookup_handle("topic_lookup", {
        "topic_id": first_topic_id,
        "include_children": True,
    })
    assert detail_result is not None
    detail_payload = json.loads(detail_result[0].text)
    assert detail_payload.get("status") == "ok"
    topic = detail_payload.get("topic", {})
    assert topic.get("id") == first_topic_id
    children = topic.get("children", [])
    assert len(children) >= 1
    assert any(int(child.get("level", 0)) == 2 for child in children)


async def test_topic_lookup_invalid_request():
    result = await topic_lookup_handle("topic_lookup", {})
    assert result is not None
    payload = json.loads(result[0].text)
    assert payload.get("status") == "invalid_request"


async def test_topic_lookup_not_found_by_topic_id():
    result = await topic_lookup_handle("topic_lookup", {
        "topic_id": str(ObjectId()),
        "include_children": True,
    })
    assert result is not None
    payload = json.loads(result[0].text)
    assert payload.get("status") == "not_found"
