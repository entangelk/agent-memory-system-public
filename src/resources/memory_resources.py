"""
MCP Resources:
  user://profile              -> user profile
  memory://recent             -> recent memory timeline
  memory://stats              -> memory statistics
  memory://compaction-status  -> compaction status by level
"""
import json
from datetime import datetime, timedelta, UTC
from mcp.server import Server
from mcp.server.lowlevel.helper_types import ReadResourceContents
from mcp.types import Resource, TextContent
from src.db import collections as col
from src.engine.compaction_engine import get_compaction_status
from src.engine import sensitivity_engine


def register(server: Server) -> None:

    @server.list_resources()
    async def list_resources() -> list[Resource]:
        return [
            Resource(
                uri="user://profile",
                name="User Profile",
                description="Accumulated user profile summary",
                mimeType="application/json",
            ),
            Resource(
                uri="memory://recent",
                name="Recent Memories",
                description="Timeline of memories saved in the last 7 days",
                mimeType="application/json",
            ),
            Resource(
                uri="memory://stats",
                name="Memory Stats",
                description="Total memory count, category distribution, pending count, and more",
                mimeType="application/json",
            ),
            Resource(
                uri="memory://compaction-status",
                name="Compaction Status",
                description="Uncompacted counts, thresholds, and readiness per compaction level",
                mimeType="application/json",
            ),
        ]

    @server.read_resource()
    async def read_resource(uri: str) -> list[ReadResourceContents]:
        uri_str = str(uri)

        if uri_str == "user://profile":
            return [ReadResourceContents(content=await _profile(), mime_type="application/json")]
        if uri_str == "memory://recent":
            return [ReadResourceContents(content=await _recent(), mime_type="application/json")]
        if uri_str == "memory://stats":
            return [ReadResourceContents(content=await _stats(), mime_type="application/json")]
        if uri_str == "memory://compaction-status":
            return [ReadResourceContents(content=await _compaction_status(), mime_type="application/json")]
        raise ValueError(f"Unknown resource: {uri_str}")


async def _profile() -> str:
    profile = await col.profiles().find_one({"user_id": "primary"})
    if not profile:
        return json.dumps({"message": "No profile found."}, ensure_ascii=False)
    profile.pop("_id", None)
    return json.dumps(profile, default=str, ensure_ascii=False, indent=2)


async def _recent() -> str:
    policy = await sensitivity_engine.load_policy()
    since = datetime.now(UTC) - timedelta(days=7)
    cursor = col.memories().find(
        {"created_at": {"$gte": since}},
        sort=[("created_at", -1)],
        limit=50,
    )
    docs = await cursor.to_list(length=50)
    result = []
    for doc in docs:
        sensitivity = sensitivity_engine.normalize_sensitivity(doc.get("sensitivity"))
        is_redacted = sensitivity_engine.should_hide_content(
            sensitivity=sensitivity,
            policy=policy,
            include_sensitive=False,
        )
        result.append({
            "id": str(doc["_id"]),
            "content": doc["content"] if not is_redacted else "[Sensitive content hidden]",
            "category": doc.get("category"),
            "importance": doc.get("importance"),
            "sensitivity": sensitivity,
            "is_redacted": is_redacted,
            "created_at": str(doc.get("created_at", "")),
        })
    return json.dumps(result, ensure_ascii=False, indent=2)


async def _stats() -> str:
    total = await col.memories().count_documents({})
    pending = await col.pending_memories().count_documents({"status": "pending"})

    pipeline = [{"$group": {"_id": "$category", "count": {"$sum": 1}}}]
    cursor = col.memories().aggregate(pipeline)
    cat_docs = await cursor.to_list(length=20)
    categories = {d["_id"]: d["count"] for d in cat_docs}

    stats = {
        "total_memories": total,
        "pending_approval": pending,
        "by_category": categories,
    }
    return json.dumps(stats, ensure_ascii=False, indent=2)


async def _compaction_status() -> str:
    status = await get_compaction_status()
    return json.dumps(status, ensure_ascii=False, indent=2)
