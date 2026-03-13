"""
memory_approve: approve or dismiss pending memories.

- approve: move a pending item into memories immediately
- dismiss: mark a pending item as dismissed
- list_pending: show the current pending queue
"""
from datetime import datetime, UTC
from bson import ObjectId
from mcp.types import Tool, TextContent
from src.db import collections as col
from src.engine.memory_engine import save_memory
from src.engine import sensitivity_engine


def get_tools() -> list[Tool]:
    return [
        Tool(
            name="memory_approve",
            description=(
                "Manage pending memories. "
                "Supported actions: list, approve, dismiss."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["list", "approve", "dismiss"],
                        "description": "Action to perform",
                    },
                    "pending_id": {
                        "type": "string",
                        "description": "pending_memory _id to process (required for approve and dismiss)",
                    },
                    "include_sensitive": {
                        "type": "boolean",
                        "description": "Include full sensitive content in list results when true",
                    },
                },
                "required": ["action"],
            },
        )
    ]


async def handle(name: str, args: dict) -> list[TextContent] | None:
    if name != "memory_approve":
        return None

    action = args["action"]
    include_sensitive = bool(args.get("include_sensitive", False))

    if action == "list":
        return await _list_pending(include_sensitive=include_sensitive)
    if action == "approve":
        return await _approve(args.get("pending_id", ""))
    if action == "dismiss":
        return await _dismiss(args.get("pending_id", ""))
    return [TextContent(type="text", text=f"Unknown action: {action}")]


async def _list_pending(*, include_sensitive: bool = False) -> list[TextContent]:
    policy = await sensitivity_engine.load_policy()
    cursor = col.pending_memories().find(
        {"status": "pending"},
        sort=[("importance", -1)],
        limit=20,
    )
    docs = await cursor.to_list(length=20)

    if not docs:
        return [TextContent(type="text", text="There are no pending memories.")]

    ids = [d["_id"] for d in docs]
    await col.pending_memories().update_many(
        {"_id": {"$in": ids}, "presented_at": None},
        {"$set": {"presented_at": datetime.now(UTC)}},
    )

    lines = [f"Pending memories ({len(docs)}):\n"]
    for doc in docs:
        suggested_sensitivity = sensitivity_engine.normalize_sensitivity(doc.get("suggested_sensitivity"))
        is_redacted = sensitivity_engine.should_hide_content(
            sensitivity=suggested_sensitivity,
            policy=policy,
            include_sensitive=include_sensitive,
        )
        content = doc["content"] if not is_redacted else "[Sensitive content hidden] Re-run with include_sensitive=true."
        lines.append(
            f"- id={doc['_id']} | [{doc.get('suggested_category', '?')}|importance:{doc.get('importance', 0)}|sensitivity:{suggested_sensitivity}]\n"
            f"  {content}"
        )

    return [TextContent(type="text", text="\n".join(lines))]


async def _approve(pending_id: str) -> list[TextContent]:
    if not pending_id:
        return [TextContent(type="text", text="pending_id is required.")]

    try:
        oid = ObjectId(pending_id)
    except Exception:
        return [TextContent(type="text", text=f"Invalid id: {pending_id}")]

    doc = await col.find_by_id(col.pending_memories(), oid)
    if not doc:
        return [TextContent(type="text", text="Pending memory not found.")]
    if doc.get("status") != "pending":
        return [TextContent(type="text", text=f"This item has already been processed. (status={doc.get('status')})")]

    memory_id = await save_memory(
        content=doc["content"],
        category=doc.get("suggested_category", "fact"),
        importance=doc.get("importance", 7),
        sensitivity=doc.get("suggested_sensitivity", "normal"),
        entities=doc.get("suggested_entities", []),
    )

    await col.update_one(
        col.pending_memories(),
        oid,
        {"$set": {"status": "approved"}},
    )

    return [TextContent(type="text", text=f"Approved and saved as memory (id={memory_id})")]


async def _dismiss(pending_id: str) -> list[TextContent]:
    if not pending_id:
        return [TextContent(type="text", text="pending_id is required.")]

    try:
        oid = ObjectId(pending_id)
    except Exception:
        return [TextContent(type="text", text=f"Invalid id: {pending_id}")]

    doc = await col.find_by_id(col.pending_memories(), oid)
    if not doc:
        return [TextContent(type="text", text="Pending memory not found.")]
    if doc.get("status") != "pending":
        return [TextContent(type="text", text=f"This item has already been processed. (status={doc.get('status')})")]

    await col.update_one(
        col.pending_memories(),
        oid,
        {"$set": {"status": "dismissed"}},
    )

    return [TextContent(type="text", text="Dismissed. This memory was not saved.")]
