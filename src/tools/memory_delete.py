"""
memory_delete: remove an existing memory document.
"""
import json
from bson import ObjectId
from mcp.types import Tool, TextContent
from src.db import collections as col


def get_tools() -> list[Tool]:
    return [
        Tool(
            name="memory_delete",
            description="Delete a stored memory.",
            inputSchema={
                "type": "object",
                "properties": {
                    "memory_id": {
                        "type": "string",
                        "description": "The _id of the memory to delete",
                    },
                },
                "required": ["memory_id"],
            },
        )
    ]


async def handle(name: str, args: dict) -> list[TextContent] | None:
    if name != "memory_delete":
        return None

    memory_id = args.get("memory_id", "")
    if not memory_id:
        return [TextContent(type="text", text="memory_id is required.")]

    try:
        oid = ObjectId(memory_id)
    except Exception:
        return [TextContent(type="text", text=f"Invalid id: {memory_id}")]

    doc = await col.find_by_id(col.memories(), oid)
    if not doc:
        return [TextContent(type="text", text="Memory not found.")]

    topic_id = doc.get("topic_id")
    await col.memories().delete_one({"_id": oid})

    topic_memory_count: int | None = None
    if topic_id is not None:
        topic_memory_count = await col.memories().count_documents({"topic_id": topic_id})
        await col.update_one(
            col.topics(),
            topic_id,
            {"$set": {"memory_count": int(topic_memory_count)}},
        )

    return [TextContent(
        type="text",
        text=json.dumps({
            "id": memory_id,
            "status": "deleted",
            "topic_memory_count": topic_memory_count,
        }, ensure_ascii=False),
    )]
