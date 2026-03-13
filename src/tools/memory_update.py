"""
memory_update: partially update an existing memory document.
"""
import json
from bson import ObjectId
from mcp.types import Tool, TextContent
from src.db import collections as col
from src.engine import topic_engine, residual_engine, sensitivity_engine


_UPDATABLE_FIELDS = {"content", "category", "importance", "context", "entities", "residual_info", "sensitivity"}


def get_tools() -> list[Tool]:
    return [
        Tool(
            name="memory_update",
            description="Partially update a stored memory.",
            inputSchema={
                "type": "object",
                "properties": {
                    "memory_id": {
                        "type": "string",
                        "description": "The _id of the memory to update",
                    },
                    "content": {"type": "string"},
                    "category": {
                        "type": "string",
                        "enum": ["preference", "fact", "event", "emotion", "digest"],
                    },
                    "importance": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 10,
                    },
                    "sensitivity": {
                        "type": "string",
                        "enum": ["normal", "medium", "high"],
                    },
                    "context": {"type": "string"},
                    "entities": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "residual_info": {
                        "type": "object",
                        "additionalProperties": True,
                    },
                },
                "required": ["memory_id"],
            },
        )
    ]


async def handle(name: str, args: dict) -> list[TextContent] | None:
    if name != "memory_update":
        return None

    memory_id = args.get("memory_id", "")
    if not memory_id:
        return [TextContent(type="text", text="memory_id is required.")]

    try:
        oid = ObjectId(memory_id)
    except Exception:
        return [TextContent(type="text", text=f"Invalid id: {memory_id}")]

    existing = await col.find_by_id(col.memories(), oid)
    if not existing:
        return [TextContent(type="text", text="Memory not found.")]

    set_fields: dict = {}
    for field in _UPDATABLE_FIELDS:
        if field in args:
            set_fields[field] = args[field]

    if "residual_info" in set_fields:
        normalized_residual_info = residual_engine.normalize_residual_info(set_fields["residual_info"])
        set_fields["residual_info"] = normalized_residual_info
        set_fields["residual_slots"] = residual_engine.residual_slots(normalized_residual_info)
    if "sensitivity" in set_fields:
        set_fields["sensitivity"] = sensitivity_engine.normalize_sensitivity(set_fields["sensitivity"])

    if not set_fields:
        return [TextContent(type="text", text="No updatable fields were provided.")]

    await col.update_one(col.memories(), oid, {"$set": set_fields})

    entities = set_fields.get("entities")
    if isinstance(entities, list) and entities:
        await topic_engine.maybe_create_topic(entities)

    updated = await col.find_by_id(col.memories(), oid)
    if updated and updated.get("topic_id") is not None:
        await residual_engine.sync_memory_residual_mappings(
            memory_id=oid,
            topic_id=updated.get("topic_id"),
            residual_info=updated.get("residual_info"),
        )

    return [TextContent(
        type="text",
        text=json.dumps({
            "id": memory_id,
            "status": "updated",
            "updated_fields": sorted(set_fields.keys()),
        }, ensure_ascii=False),
    )]
