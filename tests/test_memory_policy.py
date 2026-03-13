"""
memory_policy 통합 테스트.
MongoDB가 localhost:27017에서 실행 중이어야 합니다.
"""
import json
from datetime import datetime, UTC
import pytest
from src.db.connection import get_db
from src.engine import sensitivity_engine
from src.tools.memory_policy import handle


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


async def test_memory_policy_unknown_tool_returns_none():
    result = await handle("unknown_tool", {})
    assert result is None


async def test_memory_policy_get_returns_policy():
    result = await handle("memory_policy", {"action": "get"})
    assert result is not None
    payload = json.loads(result[0].text)
    assert payload.get("status") == "ok"
    policy = payload.get("policy", {})
    assert policy.get("hide_sensitive_on_recall") is False
    assert isinstance(policy.get("agent_instruction"), str)


async def test_memory_policy_set_updates_instruction():
    result = await handle("memory_policy", {
        "action": "set",
        "hide_sensitive_on_recall": True,
        "agent_instruction": "The agent should judge sensitivity contextually and hide only high-sensitivity details by default.",
    })
    assert result is not None
    payload = json.loads(result[0].text)
    assert payload.get("status") == "updated"
    policy = payload.get("policy", {})
    assert policy.get("hide_sensitive_on_recall") is True
    assert policy.get("agent_instruction") == "The agent should judge sensitivity contextually and hide only high-sensitivity details by default."
