import json
from bson import ObjectId
from mcp.types import Tool, TextContent
from src.engine.memory_engine import save_memory, mark_compacted_sources, upsert_digest_record
from src.engine.compaction_engine import get_compaction_hint
from src.engine import topic_engine, sensitivity_engine
from src.db import collections as col


def get_tools() -> list[Tool]:
    return [
        Tool(
            name="memory_save",
            description=(
                "Save a memory to the long-term memory store. "
                "The content is stored as provided and may be returned verbatim during recall. "
                "Clients should extract and summarize the essential information before saving. "
                "Storing full conversations will reduce retrieval quality and make recall responses noisy. "
                "Short notes can be stored as-is, but longer content should be summarized first."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "content": {"type": "string", "description": "Memory content to save. Keep it concise when possible (roughly 1-3 sentences). Summarize long content before saving."},
                    "category": {
                        "type": "string",
                        "enum": ["preference", "fact", "event", "emotion", "digest"],
                        "description": "Memory category",
                    },
                    "importance": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 10,
                        "description": "Importance score (1-10)",
                    },
                    "sensitivity": {
                        "type": "string",
                        "enum": ["normal", "medium", "high"],
                        "description": "Sensitivity level (optional, set explicitly by the agent; default: normal)",
                    },
                    "context": {"type": "string", "description": "Additional context (optional)"},
                    "entities": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Related entities (optional)",
                    },
                    "residual_info": {
                        "type": "object",
                        "description": "Residual details not captured in the main summary (flexible key/value, optional)",
                        "additionalProperties": True,
                    },
                    "centroid_topic_id": {
                        "type": "string",
                        "description": "Centroid topic _id to update with a compaction result (optional)",
                    },
                    "centroid_summary": {
                        "type": "string",
                        "description": "Summary text to apply to the centroid topic (optional)",
                    },
                    "compacted_source_ids": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Source memory _ids when saving a digest (optional)",
                    },
                    "digest_type": {
                        "type": "string",
                        "enum": ["daily", "weekly", "monthly", "yearly"],
                        "description": "Aggregation level type when saving a digest (optional)",
                    },
                    "digest_period": {
                        "type": "string",
                        "description": "Period key when saving a digest (for example: 2026-02-23, 2026-W08, 2026-02, 2025)",
                    },
                    "topic_path": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Topic hierarchy path (T1->T4). Example: [task, project, domain, broad category] (optional)",
                    },
                    "topic_aliases": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Aliases to add to the first node (T1) of topic_path (optional)",
                    },
                },
                "required": ["content", "category", "importance"],
            },
        )
    ]


async def handle(name: str, args: dict) -> list[TextContent] | None:
    if name != "memory_save":
        return None
    selected_sensitivity = sensitivity_engine.normalize_sensitivity(args.get("sensitivity"))

    oid = await save_memory(
        content=args["content"],
        category=args["category"],
        importance=args["importance"],
        sensitivity=selected_sensitivity,
        context=args.get("context", ""),
        entities=args.get("entities", []),
        residual_info=args.get("residual_info"),
    )
    result = {"id": str(oid), "status": "saved", "sensitivity": selected_sensitivity}

    compacted_source_ids = args.get("compacted_source_ids", [])
    source_oids: list[ObjectId] = []
    if isinstance(compacted_source_ids, list):
        for source_id in compacted_source_ids:
            if not isinstance(source_id, str):
                continue
            try:
                source_oids.append(ObjectId(source_id))
            except Exception:
                continue

    if args["category"] == "digest" and source_oids:
        updated_counts = await mark_compacted_sources(source_ids=source_oids, digest_id=oid)
        result["compacted_sources_updated"] = updated_counts["total_updated"]
        result["compacted_memories_updated"] = updated_counts["memories_updated"]
        result["compacted_digests_updated"] = updated_counts["digests_updated"]

    topic_path_raw = args.get("topic_path", [])
    topic_aliases_raw = args.get("topic_aliases", [])
    topic_aliases = topic_aliases_raw if isinstance(topic_aliases_raw, list) else []

    centroid_topic_id = args.get("centroid_topic_id", "")
    centroid_summary = args.get("centroid_summary", "")
    needs_topic_inference = bool(centroid_summary) or (isinstance(topic_path_raw, list) and bool(topic_path_raw))
    if not centroid_topic_id and needs_topic_inference and source_oids:
        inferred_topic_id = await topic_engine.infer_dominant_topic_id_from_memories(source_oids)
        if inferred_topic_id:
            centroid_topic_id = inferred_topic_id
            result["centroid_topic_id_inferred"] = inferred_topic_id

    topic_path_nodes: list[dict] = []
    if isinstance(topic_path_raw, list) and topic_path_raw:
        topic_path_nodes = await topic_engine.ensure_topic_path(
            topic_path=topic_path_raw,
            base_topic_id=centroid_topic_id or None,
            base_aliases=topic_aliases,
        )
        if topic_path_nodes:
            result["topic_path_nodes"] = topic_path_nodes
            saved_doc = await col.find_by_id(col.memories(), oid)
            first_topic_id = topic_path_nodes[0].get("topic_id", "")
            if saved_doc and saved_doc.get("topic_id") is None and isinstance(first_topic_id, str) and first_topic_id:
                try:
                    await col.update_one(col.memories(), oid, {"$set": {"topic_id": ObjectId(first_topic_id)}})
                    result["memory_topic_assigned"] = first_topic_id
                except Exception:
                    pass

    digest_record_id: str | None = None
    if args["category"] == "digest":
        digest_type = args.get("digest_type", "")
        digest_period = args.get("digest_period", "")
        digest_topic_id = centroid_topic_id
        selected_topic_id = topic_engine.select_topic_id_for_digest_type(topic_path_nodes, digest_type)
        if selected_topic_id:
            digest_topic_id = selected_topic_id
            result["digest_topic_id"] = selected_topic_id
        if digest_type and digest_period:
            digest_id = await upsert_digest_record(
                digest_type=digest_type,
                period=digest_period,
                content=args["content"],
                source_ids=source_oids,
                topic_id=digest_topic_id,
            )
            digest_record_id = str(digest_id)
            result["digest_record_id"] = digest_record_id
            result["digest_record_upserted"] = True

    if centroid_topic_id and centroid_summary:
        source_digest_ids = [digest_record_id] if digest_record_id else None
        updated = await topic_engine.update_centroid_summary(
            centroid_topic_id,
            centroid_summary,
            source_digest_ids=source_digest_ids,
        )
        result["centroid_updated"] = bool(updated)

    # Include compaction readiness hints when any level is ready.
    hint = await get_compaction_hint()
    if hint:
        result["compaction_hint"] = hint

    return [TextContent(type="text", text=json.dumps(result, ensure_ascii=False))]
