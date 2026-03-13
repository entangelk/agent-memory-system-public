"""
MCP 서버 인터페이스(도구/리소스) 계약 스모크 테스트.
MongoDB가 localhost:27017에서 실행 중이어야 합니다.
"""
import json
import pytest
from bson import ObjectId
from mcp import types
from src.db.connection import get_db
from src.db import collections as col
from src.server import app

TEST_CONTEXT = "mcp_server_contract_ctx"
TEST_SESSION = "mcp_server_contract_session"
CONTENT_TOKEN = "mcp_server_contract_token"

EXPECTED_TOOLS = {
    "memory_save",
    "memory_recall",
    "memory_summarize",
    "session_digest",
    "memory_approve",
    "memory_compact",
    "memory_update",
    "memory_delete",
    "topic_lookup",
    "memory_policy",
}

EXPECTED_RESOURCES = {
    "user://profile",
    "memory://recent",
    "memory://stats",
    "memory://compaction-status",
}


def _first_text(content_blocks: list) -> str:
    for block in content_blocks:
        text = getattr(block, "text", None)
        if isinstance(text, str):
            return text
    return ""


async def _call_tool(name: str, arguments: dict) -> types.CallToolResult:
    list_tools_handler = app.request_handlers[types.ListToolsRequest]
    call_tool_handler = app.request_handlers[types.CallToolRequest]

    # tool cache를 항상 최신으로 유지
    await list_tools_handler(types.ListToolsRequest())
    result = await call_tool_handler(
        types.CallToolRequest(
            params=types.CallToolRequestParams(name=name, arguments=arguments),
        )
    )
    assert isinstance(result.root, types.CallToolResult)
    return result.root


@pytest.fixture(autouse=True)
async def cleanup():
    db = get_db()
    await db["memories"].delete_many({"context": TEST_CONTEXT})
    await db["pending_memories"].delete_many({"source_session": TEST_SESSION})
    yield
    await db["memories"].delete_many({"context": TEST_CONTEXT})
    await db["pending_memories"].delete_many({"source_session": TEST_SESSION})


async def test_mcp_server_lists_expected_tools_and_resources():
    list_tools_handler = app.request_handlers[types.ListToolsRequest]
    list_resources_handler = app.request_handlers[types.ListResourcesRequest]

    tools_result = await list_tools_handler(types.ListToolsRequest())
    assert isinstance(tools_result.root, types.ListToolsResult)
    tool_names = {tool.name for tool in tools_result.root.tools}
    assert tool_names == EXPECTED_TOOLS

    resources_result = await list_resources_handler(types.ListResourcesRequest())
    assert isinstance(resources_result.root, types.ListResourcesResult)
    resource_uris = {str(resource.uri) for resource in resources_result.root.resources}
    assert resource_uris == EXPECTED_RESOURCES


async def test_mcp_server_smoke_calls_tools_and_reads_resources():
    policy_get = await _call_tool("memory_policy", {"action": "get"})
    assert policy_get.isError is False
    policy_get_payload = json.loads(_first_text(policy_get.content))
    assert policy_get_payload.get("status") == "ok"

    policy_set = await _call_tool("memory_policy", {
        "action": "set",
        "hide_sensitive_on_recall": True,
        "agent_instruction": "The agent should judge sensitivity contextually and hide only high-sensitivity details by default.",
    })
    assert policy_set.isError is False
    policy_set_payload = json.loads(_first_text(policy_set.content))
    assert policy_set_payload.get("status") == "updated"

    save_result = await _call_tool("memory_save", {
        "content": f"{CONTENT_TOKEN} save",
        "category": "fact",
        "importance": 6,
        "context": TEST_CONTEXT,
        "entities": ["mcp_server_contract_entity"],
        "topic_path": ["mcp contract topic", "mcp contract group"],
    })
    assert save_result.isError is False
    save_payload = json.loads(_first_text(save_result.content))
    assert save_payload["status"] == "saved"
    saved_doc = await col.memories().find_one({"content": f"{CONTENT_TOKEN} save", "context": TEST_CONTEXT})
    assert saved_doc is not None
    memory_id = str(saved_doc["_id"])

    recall_result = await _call_tool("memory_recall", {"query": CONTENT_TOKEN})
    assert recall_result.isError is False
    recall_text = _first_text(recall_result.content)
    assert "Results" in recall_text

    summarize_result = await _call_tool("memory_summarize", {"category": "fact", "limit": 5})
    assert summarize_result.isError is False
    summarize_text = _first_text(summarize_result.content)
    assert "Memory summary" in summarize_text or "No memories matched the requested filters." in summarize_text

    topic_lookup_result = await _call_tool("topic_lookup", {"query": "mcp contract topic", "top_k": 5})
    assert topic_lookup_result.isError is False
    topic_lookup_payload = json.loads(_first_text(topic_lookup_result.content))
    assert topic_lookup_payload.get("status") == "ok"
    assert topic_lookup_payload.get("count", 0) >= 1

    digest_result = await _call_tool("session_digest", {
        "session_id": TEST_SESSION,
        "candidates": [
            {"content": "ignored candidate", "category": "fact", "importance": 2, "entities": []},
        ],
    })
    assert digest_result.isError is False
    assert "ignored: 1" in _first_text(digest_result.content)

    approve_result = await _call_tool("memory_approve", {"action": "list"})
    assert approve_result.isError is False

    compact_result = await _call_tool("memory_compact", {})
    assert compact_result.isError is False
    compact_payload = json.loads(_first_text(compact_result.content))
    assert "L1_daily" in compact_payload

    update_result = await _call_tool("memory_update", {"memory_id": memory_id, "importance": 7})
    assert update_result.isError is False
    update_payload = json.loads(_first_text(update_result.content))
    assert update_payload["status"] == "updated"

    delete_result = await _call_tool("memory_delete", {"memory_id": memory_id})
    assert delete_result.isError is False
    delete_payload = json.loads(_first_text(delete_result.content))
    assert delete_payload["status"] == "deleted"

    deleted_doc = await col.find_by_id(col.memories(), ObjectId(memory_id))
    assert deleted_doc is None

    policy_reset = await _call_tool("memory_policy", {
        "action": "set",
        "hide_sensitive_on_recall": False,
        "agent_instruction": "Hide sensitive details in default responses, and only re-run with include_sensitive=true when the user's request is explicit.",
    })
    assert policy_reset.isError is False

    list_resources_handler = app.request_handlers[types.ListResourcesRequest]
    read_resource_handler = app.request_handlers[types.ReadResourceRequest]
    resources_result = await list_resources_handler(types.ListResourcesRequest())
    assert isinstance(resources_result.root, types.ListResourcesResult)

    for resource in resources_result.root.resources:
        read_result = await read_resource_handler(
            types.ReadResourceRequest(
                params=types.ReadResourceRequestParams(uri=str(resource.uri)),
            )
        )
        assert isinstance(read_result.root, types.ReadResourceResult)
        assert len(read_result.root.contents) > 0
        payload_text = getattr(read_result.root.contents[0], "text", "")
        assert isinstance(payload_text, str)
        json.loads(payload_text)
