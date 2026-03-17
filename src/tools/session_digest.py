"""
session_digest: extract memory candidates from a conversation and route them.

Mode A (importance >= 10): save directly to memories
Mode B (importance 7-9):   save to pending_memories for later approval
Mode C (importance < 7):   ignore
"""
from datetime import datetime, UTC
from mcp.types import Tool, TextContent
from src.db import collections as col
from src.engine.memory_engine import save_memory
from src.engine import sensitivity_engine


def get_tools() -> list[Tool]:
    return [
        Tool(
            name="session_digest",
            description=(
                "Process memory candidates extracted from a conversation session. "
                "Use this when a longer interaction yields several candidate memories to triage at once. "
                "Importance 10 saves immediately, 7-9 goes to pending approval, and anything below 7 is ignored."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "session_id": {
                        "type": "string",
                        "description": "Session identifier",
                    },
                    "candidates": {
                        "type": "array",
                        "description": "Memory candidate list",
                        "items": {
                            "type": "object",
                            "properties": {
                                "content": {"type": "string"},
                                "category": {
                                    "type": "string",
                                    "enum": ["preference", "fact", "event", "emotion"],
                                },
                                "importance": {"type": "integer", "minimum": 1, "maximum": 10},
                                "sensitivity": {
                                    "type": "string",
                                    "enum": ["normal", "medium", "high"],
                                    "description": "Sensitivity level (optional, set by the agent)",
                                },
                                "entities": {"type": "array", "items": {"type": "string"}},
                            },
                            "required": ["content", "category", "importance"],
                        },
                    },
                },
                "required": ["candidates"],
            },
        )
    ]


async def handle(name: str, args: dict) -> list[TextContent] | None:
    if name != "session_digest":
        return None

    session_id = args.get("session_id", "")
    candidates: list[dict] = args.get("candidates", [])

    saved_count = 0
    pending_count = 0
    ignored_count = 0

    for cand in candidates:
        importance = cand["importance"]
        selected_sensitivity = sensitivity_engine.normalize_sensitivity(cand.get("sensitivity"))

        if importance >= 10:
            await save_memory(
                content=cand["content"],
                category=cand["category"],
                importance=importance,
                sensitivity=selected_sensitivity,
                entities=cand.get("entities", []),
            )
            saved_count += 1

        elif importance >= 7:
            await col.insert_one(
                col.pending_memories(),
                {
                    "content": cand["content"],
                    "importance": importance,
                    "suggested_sensitivity": selected_sensitivity,
                    "source_session": session_id,
                    "suggested_category": cand["category"],
                    "suggested_entities": cand.get("entities", []),
                    "status": "pending",
                    "presented_at": None,
                    "created_at": datetime.now(UTC),
                    "updated_at": datetime.now(UTC),
                },
            )
            pending_count += 1

        else:
            ignored_count += 1

    summary = (
        f"Processed — saved immediately: {saved_count}, pending approval: {pending_count}, ignored: {ignored_count}"
    )
    return [TextContent(type="text", text=summary)]
