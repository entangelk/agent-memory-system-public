import json
from mcp.types import Tool, TextContent
from src.engine.compaction_engine import get_compaction_status, fetch_compaction_sources
from src.engine import sensitivity_engine


def get_tools() -> list[Tool]:
    return [
        Tool(
            name="memory_compact",
            description=(
                "Return source memories or digests that are ready for compaction. "
                "The client should summarize them and save the result through memory_save. "
                "When level is omitted, the tool returns status only."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "level": {
                        "type": "string",
                        "enum": ["L1_daily", "L2_weekly", "L3_monthly", "L4_yearly"],
                        "description": "Compaction level. If omitted, only status is returned.",
                    },
                    "include_sensitive": {
                        "type": "boolean",
                        "description": "Include full sensitive source content when true",
                    },
                },
                "required": [],
            },
        )
    ]


async def handle(name: str, args: dict) -> list[TextContent] | None:
    if name != "memory_compact":
        return None

    level = args.get("level")
    include_sensitive = bool(args.get("include_sensitive", False))

    if not level:
        status = await get_compaction_status()
        return [TextContent(
            type="text",
            text=json.dumps(status, ensure_ascii=False, indent=2),
        )]

    sources = await fetch_compaction_sources(level)
    if not sources:
        return [TextContent(
            type="text",
            text=json.dumps({"level": level, "sources": [], "message": "No compaction candidates found."}, ensure_ascii=False),
        )]

    centroid_counts: dict[str, int] = {}
    for src in sources:
        topic_id = src.get("topic_id")
        if topic_id is None:
            continue
        key = str(topic_id)
        centroid_counts[key] = centroid_counts.get(key, 0) + 1

    centroid_targets = [
        {"topic_id": topic_id, "source_count": count}
        for topic_id, count in sorted(centroid_counts.items(), key=lambda x: x[1], reverse=True)
    ]
    policy = await sensitivity_engine.load_policy()
    redacted_sources: list[dict] = []
    redacted_count = 0
    for src in sources:
        row = dict(src)
        sensitivity = sensitivity_engine.normalize_sensitivity(row.get("sensitivity"))
        row["sensitivity"] = sensitivity
        if sensitivity_engine.should_hide_content(
            sensitivity=sensitivity,
            policy=policy,
            include_sensitive=include_sensitive,
        ):
            if "content" in row:
                row["content"] = "[Sensitive content hidden] Re-run with include_sensitive=true."
            row["is_redacted"] = True
            redacted_count += 1
        else:
            row["is_redacted"] = False
        redacted_sources.append(row)

    digest_type_map = {
        "L1_daily": "daily",
        "L2_weekly": "weekly",
        "L3_monthly": "monthly",
        "L4_yearly": "yearly",
    }

    return [TextContent(
        type="text",
        text=json.dumps({
            "level": level,
            "digest_type": digest_type_map.get(level, ""),
            "count": len(sources),
            "redacted_count": redacted_count,
            "sources": redacted_sources,
            "centroid_targets": centroid_targets,
            "instruction": (
                "Summarize these memories, then save the result with "
                "memory_save(category='digest', digest_type=..., digest_period=..., compacted_source_ids=[...], ...). "
                "If you also provide centroid_summary, the server can update the topic summary using "
                "centroid_topic_id or source-based inference."
            ),
        }, ensure_ascii=False, default=str, indent=2),
    )]
