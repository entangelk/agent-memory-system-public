from mcp.types import Tool, TextContent
from src.db import collections as col
from src.engine import sensitivity_engine


def get_tools() -> list[Tool]:
    return [
        Tool(
            name="memory_summarize",
            description="Return a summary-style listing of memories for a category or topic.",
            inputSchema={
                "type": "object",
                "properties": {
                    "category": {
                        "type": "string",
                        "enum": ["preference", "fact", "event", "emotion"],
                        "description": "Category to summarize (optional)",
                    },
                    "topic_slug": {
                        "type": "string",
                        "description": "Topic slug to summarize (optional)",
                    },
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 50,
                        "description": "Number of memories to fetch (default: 10)",
                    },
                    "include_sensitive": {
                        "type": "boolean",
                        "description": "Include full sensitive content when true",
                    },
                },
                "required": [],
            },
        )
    ]


async def handle(name: str, args: dict) -> list[TextContent] | None:
    if name != "memory_summarize":
        return None

    category = args.get("category")
    topic_slug = args.get("topic_slug")
    limit = args.get("limit", 10)
    include_sensitive = bool(args.get("include_sensitive", False))
    policy = await sensitivity_engine.load_policy()

    mongo_filter: dict = {}
    if category:
        mongo_filter["category"] = category

    if topic_slug:
        topic = await col.topics().find_one({"slug": topic_slug})
        if topic:
            mongo_filter["topic_id"] = topic["_id"]
        else:
            return [TextContent(type="text", text=f"Topic '{topic_slug}' was not found.")]

    cursor = col.memories().find(mongo_filter, sort=[("importance", -1)], limit=limit)
    docs = await cursor.to_list(length=limit)

    if not docs:
        return [TextContent(type="text", text="No memories matched the requested filters.")]

    lines = [f"Memory summary ({len(docs)}):\n"]
    for doc in docs:
        sensitivity = sensitivity_engine.normalize_sensitivity(doc.get("sensitivity"))
        is_redacted = sensitivity_engine.should_hide_content(
            sensitivity=sensitivity,
            policy=policy,
            include_sensitive=include_sensitive,
        )
        content = doc["content"] if not is_redacted else "[Sensitive content hidden] Re-run with include_sensitive=true."
        lines.append(
            f"- [{doc.get('category', '?')}|importance:{doc.get('importance', 0)}|sensitivity:{sensitivity}] {content}"
        )

    return [TextContent(type="text", text="\n".join(lines))]
