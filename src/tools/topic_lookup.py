"""
topic_lookup: search or inspect hierarchical topic records.
"""
import json
from mcp.types import Tool, TextContent
from src.engine import topic_engine


def get_tools() -> list[Tool]:
    return [
        Tool(
            name="topic_lookup",
            description="Search topics or fetch topic details by topic_id.",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Search query (optional, but either query or topic_id is required)",
                    },
                    "topic_id": {
                        "type": "string",
                        "description": "Topic _id to inspect in detail (optional)",
                    },
                    "level": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 4,
                        "description": "Topic level filter (optional)",
                    },
                    "top_k": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 50,
                        "description": "Maximum number of search results (default: 10)",
                    },
                    "include_children": {
                        "type": "boolean",
                        "description": "Include child topics when querying by topic_id (default: false)",
                    },
                },
                "required": [],
            },
        )
    ]


async def handle(name: str, args: dict) -> list[TextContent] | None:
    if name != "topic_lookup":
        return None

    topic_id = args.get("topic_id", "")
    include_children = bool(args.get("include_children", False))
    if isinstance(topic_id, str) and topic_id.strip():
        detail = await topic_engine.get_topic_detail(topic_id.strip(), include_children=include_children)
        if detail is None:
            return [TextContent(type="text", text=json.dumps({"status": "not_found", "topic_id": topic_id}, ensure_ascii=False))]
        return [TextContent(type="text", text=json.dumps({"status": "ok", "topic": detail}, ensure_ascii=False))]

    query = args.get("query", "")
    if not isinstance(query, str) or not query.strip():
        return [TextContent(type="text", text=json.dumps({
            "status": "invalid_request",
            "message": "Either query or topic_id is required.",
        }, ensure_ascii=False))]

    level = args.get("level")
    if not isinstance(level, int) or level <= 0:
        level = None
    top_k = args.get("top_k", 10)
    if not isinstance(top_k, int):
        top_k = 10

    topics = await topic_engine.search_topics(
        query=query,
        level=level,
        top_k=top_k,
    )
    return [TextContent(type="text", text=json.dumps({
        "status": "ok",
        "count": len(topics),
        "topics": topics,
    }, ensure_ascii=False))]
