from datetime import datetime, timedelta, UTC
from bson import ObjectId
from mcp.types import Tool, TextContent
from src import config
from src.db import collections as col
from src.engine.scoring import calculate_score, calculate_combined_score
from src.engine import chroma_engine, topic_engine, sensitivity_engine


def get_tools() -> list[Tool]:
    return [
        Tool(
            name="memory_recall",
            description=(
                "Search stored memories. "
                "Use this before answering questions about what you remember, prior conversations, or the user's past preferences, plans, facts, and events. "
                "Combines Chroma centroid and memory-vector search when enabled, "
                "and falls back to Mongo text search when unavailable."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Search query"},
                    "category": {
                        "type": "string",
                        "enum": ["preference", "fact", "event", "emotion"],
                        "description": "Category filter (optional)",
                    },
                    "time_range": {
                        "type": "string",
                        "enum": ["1d", "7d", "30d", "90d", "all"],
                        "description": "Time range filter (optional, default: all)",
                    },
                    "top_k": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 20,
                        "description": "Maximum number of results to return (default: 5)",
                    },
                    "include_debug": {
                        "type": "boolean",
                        "description": "Return JSON debug output with ids and scores when true",
                    },
                    "include_sensitive": {
                        "type": "boolean",
                        "description": "Include full sensitive content when true (default: false)",
                    },
                },
                "required": [],
            },
        )
    ]


async def handle(name: str, args: dict) -> list[TextContent] | None:
    if name != "memory_recall":
        return None

    query_raw = args.get("query", "")
    query = query_raw.strip() if isinstance(query_raw, str) else ""
    category = args.get("category")
    time_range = args.get("time_range", "all")
    top_k = args.get("top_k", 5)
    include_debug = bool(args.get("include_debug", False))
    include_sensitive = bool(args.get("include_sensitive", False))

    mongo_filter: dict = {}

    if category:
        mongo_filter["category"] = category

    since_ts: int | None = None
    if time_range != "all":
        days_map = {"1d": 1, "7d": 7, "30d": 30, "90d": 90}
        since = datetime.now(UTC) - timedelta(days=days_map[time_range])
        mongo_filter["created_at"] = {"$gte": since}
        since_ts = int(since.timestamp())

    memories_col = col.memories()
    docs: list[dict] = []
    seen_doc_ids: set[str] = set()

    def add_unique(rows: list[dict]) -> None:
        for row in rows:
            key = str(row.get("_id", ""))
            if not key or key in seen_doc_ids:
                continue
            seen_doc_ids.add(key)
            docs.append(row)

    similarity_map: dict[str, float] = {}

    topic_ids: list[str] = []
    topic_similarity: dict[str, float] = {}
    if query:
        try:
            topic_scored = await chroma_engine.search_topic_ids_with_scores(
                query=query,
                top_k=top_k * 3,
            )
            topic_ids = [tid for tid, _ in topic_scored]
            topic_similarity = {tid: sim for tid, sim in topic_scored}
        except Exception:
            topic_ids = []

    if topic_ids:
        object_topic_ids: list[ObjectId] = []
        for value in topic_ids:
            try:
                object_topic_ids.append(ObjectId(value))
            except Exception:
                continue

        vector_filter = dict(mongo_filter)
        vector_filter["topic_id"] = {"$in": object_topic_ids}
        if object_topic_ids:
            cursor = memories_col.find(vector_filter, limit=top_k * 3)
            topic_docs = await cursor.to_list(length=top_k * 3)

            rank = {topic_id: i for i, topic_id in enumerate(topic_ids)}
            topic_docs.sort(key=lambda d: rank.get(str(d.get("topic_id", "")), 10**9))
            for doc in topic_docs:
                doc_tid = str(doc.get("topic_id", ""))
                if doc_tid in topic_similarity:
                    doc_id = str(doc.get("_id", ""))
                    similarity_map[doc_id] = max(
                        similarity_map.get(doc_id, 0.0),
                        topic_similarity[doc_tid],
                    )
            add_unique(topic_docs)

    memory_filters: dict = {}
    if category:
        memory_filters["category"] = category
    if since_ts is not None:
        memory_filters["created_at_ts"] = {"$gte": since_ts}

    memory_scored: list[tuple[str, float]] = []
    if query or since_ts is not None:
        try:
            memory_scored = await chroma_engine.search_memory_ids_with_scores(
                query=query if query else None,
                top_k=top_k * 3,
                filters=memory_filters if memory_filters else None,
            )
        except Exception:
            memory_scored = []

    memory_ids = [mid for mid, _ in memory_scored]
    for mid, sim in memory_scored:
        similarity_map[mid] = max(similarity_map.get(mid, 0.0), sim)

    if memory_ids:
        memory_oids: list[ObjectId] = []
        for value in memory_ids:
            try:
                memory_oids.append(ObjectId(value))
            except Exception:
                continue

        if memory_oids:
            memory_filter = dict(mongo_filter)
            memory_filter["_id"] = {"$in": memory_oids}
            extra_docs = await memories_col.find(memory_filter, limit=top_k * 3).to_list(length=top_k * 3)
            rank = {memory_id: i for i, memory_id in enumerate(memory_ids)}
            extra_docs.sort(key=lambda d: rank.get(str(d.get("_id", "")), 10**9))
            add_unique(extra_docs)

    if not docs and query:
        mongo_filter["$text"] = {"$search": query}
        cursor = memories_col.find(mongo_filter, limit=top_k * 3)
        docs = await cursor.to_list(length=top_k * 3)

    if not docs and query and "$text" in mongo_filter:
        del mongo_filter["$text"]
        import re
        mongo_filter["content"] = {"$regex": re.escape(query), "$options": "i"}
        cursor = memories_col.find(mongo_filter, limit=top_k * 3)
        docs = await cursor.to_list(length=top_k * 3)

    if not docs and not query:
        cursor = memories_col.find(
            mongo_filter,
            sort=[("created_at", -1)],
            limit=top_k * 3,
        )
        docs = await cursor.to_list(length=top_k * 3)

    scored = sorted(
        docs,
        key=lambda d: calculate_combined_score(d, similarity_map.get(str(d.get("_id", "")), 0.0)),
        reverse=True,
    )[:top_k]

    if not scored:
        return [TextContent(type="text", text="No matching memories found.")]

    from datetime import datetime as dt
    for doc in scored:
        await col.update_one(
            col.memories(),
            doc["_id"],
            {"$inc": {"recall_count": 1}, "$set": {"last_recalled": dt.now(UTC)}},
        )

    lines: list[str] = []
    macro_lines: list[str] = []
    policy = await sensitivity_engine.load_policy()
    hide_sensitive_on_recall = bool(policy.get("hide_sensitive_on_recall", False))
    applied_redaction = hide_sensitive_on_recall and (not include_sensitive)
    redacted_count = 0

    if topic_ids:
        topic_oid_map: dict[str, ObjectId] = {}
        for value in topic_ids:
            try:
                topic_oid_map[value] = ObjectId(value)
            except Exception:
                continue

        if topic_oid_map:
            topic_docs = await col.topics().find(
                {"_id": {"$in": list(topic_oid_map.values())}},
                projection={"title": 1, "summary": 1},
            ).to_list(length=len(topic_oid_map))
            topic_doc_map = {str(doc["_id"]): doc for doc in topic_docs}

            macro_lines: list[str] = []
            for topic_id in topic_ids:
                topic_doc = topic_doc_map.get(topic_id)
                if not topic_doc:
                    continue
                title = topic_doc.get("title", "")
                summary = topic_doc.get("summary", "")
                if summary:
                    macro_lines.append(f"- {title}: {summary}")
                elif title:
                    macro_lines.append(f"- {title}")

            if macro_lines:
                lines.append("Macro context:")
                lines.extend(macro_lines[:3])
                lines.append("")

    candidate_topic_ids = []
    seen_topic_ids: set[str] = set()
    for raw_topic_id in topic_ids:
        if isinstance(raw_topic_id, str) and raw_topic_id and raw_topic_id not in seen_topic_ids:
            seen_topic_ids.add(raw_topic_id)
            candidate_topic_ids.append(raw_topic_id)
    for doc in scored:
        raw = doc.get("topic_id")
        if raw is None:
            continue
        key = str(raw)
        if key and key not in seen_topic_ids:
            seen_topic_ids.add(key)
            candidate_topic_ids.append(key)

    stale_recommendations = await topic_engine.centroid_refresh_recommendations(
        topic_ids=candidate_topic_ids,
        stale_days=config.CENTROID_STALE_DAYS,
    )
    if stale_recommendations:
        lines.append("Centroid refresh recommended:")
        for rec in stale_recommendations[:3]:
            title = rec.get("title", "")
            reasons = ",".join(rec.get("reasons", []))
            lines.append(f"- {title} ({reasons})")
        lines.append("")

    if hide_sensitive_on_recall:
        lines.append("Sensitivity policy:")
        lines.append(
            f"- hide_sensitive_on_recall=true, include_sensitive={str(include_sensitive).lower()}"
        )
        lines.append(f"- instruction: {policy.get('agent_instruction', '')}")
        if applied_redaction:
            lines.append("- High-sensitivity details are hidden in this response.")
        lines.append("")

    lines.append(f"Results ({len(scored)}):")
    lines.append("")
    for i, doc in enumerate(scored, 1):
        doc_id = str(doc.get("_id", ""))
        sim = similarity_map.get(doc_id, 0.0)
        score = calculate_combined_score(doc, sim)
        effective_sensitivity = sensitivity_engine.normalize_sensitivity(doc.get("sensitivity"))
        is_redacted = sensitivity_engine.should_hide_content(
            sensitivity=effective_sensitivity,
            policy=policy,
            include_sensitive=include_sensitive,
        )
        created_at = doc.get("created_at")
        time_str = created_at.strftime("%Y-%m-%d %H:%M") if created_at else "?"
        if is_redacted:
            redacted_count += 1
            lines.append(
                f"{i}. [{doc.get('category', '?')}] (importance:{doc.get('importance', 0)}, score:{score:.1f}, sensitivity:{effective_sensitivity}, saved:{time_str})\n"
                "   [Sensitive content hidden] Re-run with include_sensitive=true to view details.\n"
            )
            continue
        lines.append(
            f"{i}. [{doc.get('category', '?')}] (importance:{doc.get('importance', 0)}, score:{score:.1f}, sensitivity:{effective_sensitivity}, saved:{time_str})\n"
            f"   {doc['content']}\n"
        )

    if include_debug:
        debug_results = []
        for doc in scored:
            doc_id = str(doc.get("_id", ""))
            sim = similarity_map.get(doc_id, 0.0)
            effective_sensitivity = sensitivity_engine.normalize_sensitivity(doc.get("sensitivity"))
            is_redacted = sensitivity_engine.should_hide_content(
                sensitivity=effective_sensitivity,
                policy=policy,
                include_sensitive=include_sensitive,
            )
            created_at = doc.get("created_at")
            debug_results.append({
                "id": doc_id,
                "topic_id": str(doc.get("topic_id")) if doc.get("topic_id") is not None else None,
                "category": doc.get("category"),
                "importance": doc.get("importance", 0),
                "source_agent": doc.get("source_agent"),
                "source_client": doc.get("source_client"),
                "sensitivity": effective_sensitivity,
                "is_redacted": is_redacted,
                "similarity": round(sim, 4),
                "memory_score": round(calculate_score(doc), 4),
                "combined_score": round(calculate_combined_score(doc, sim), 4),
                "content": "" if is_redacted else doc.get("content", ""),
                "created_at": created_at.isoformat() if created_at else None,
            })
        payload = {
            "query": query,
            "count": len(scored),
            "redacted_count": redacted_count,
            "sensitivity_policy": {
                "hide_sensitive_on_recall": hide_sensitive_on_recall,
                "include_sensitive": include_sensitive,
                "applied_redaction": applied_redaction,
                "agent_instruction": policy.get("agent_instruction", ""),
            },
            "macro_context": macro_lines[:3],
            "centroid_refresh_recommendations": stale_recommendations,
            "results": debug_results,
        }
        import json
        return [TextContent(type="text", text=json.dumps(payload, ensure_ascii=False))]

    return [TextContent(type="text", text="\n".join(lines))]
