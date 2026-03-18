from datetime import datetime, UTC
from bson import ObjectId
from src.db import collections as col
from src.engine import topic_engine, residual_engine, sensitivity_engine, chroma_engine


async def save_memory(
    content: str,
    category: str,
    importance: int,
    context: str = "",
    entities: list[str] | None = None,
    residual_info: dict | None = None,
    sensitivity: str = "normal",
    source_session: ObjectId | None = None,
    source_agent: str | None = None,
    source_client: str | None = None,
) -> ObjectId:
    """Save a memory document to the memories collection and return its inserted _id."""
    ents = entities or []
    normalized_residual_info = residual_engine.normalize_residual_info(residual_info)
    normalized_sensitivity = sensitivity_engine.normalize_sensitivity(sensitivity)
    normalized_source_agent = source_agent.strip() if isinstance(source_agent, str) and source_agent.strip() else None
    normalized_source_client = source_client.strip() if isinstance(source_client, str) and source_client.strip() else None
    doc = {
        "content": content,
        "category": category,
        "importance": importance,
        "context": context,
        "entities": ents,
        "sensitivity": normalized_sensitivity,
        "residual_info": normalized_residual_info,
        "residual_slots": residual_engine.residual_slots(normalized_residual_info),
        "emotional_weight": 0.0,
        "source_session": source_session,
        "source_agent": normalized_source_agent,
        "source_client": normalized_source_client,
        "memory_tier": "short_term",
        "recall_count": 0,
        "last_recalled": None,
        "retention_score": 1.0,
        "topic_id": None,
        "compressed_from": [],
        "created_at": datetime.now(UTC),
        "updated_at": datetime.now(UTC),
    }
    memory_id = await col.insert_one(col.memories(), doc)

    # Try to auto-create or link a topic when entities are present.
    if ents:
        await topic_engine.maybe_create_topic(ents)

    saved_doc: dict | None = None
    if normalized_residual_info:
        saved_doc = await col.find_by_id(col.memories(), memory_id)
        if saved_doc and saved_doc.get("topic_id") is not None:
            await residual_engine.sync_memory_residual_mappings(
                memory_id=memory_id,
                topic_id=saved_doc.get("topic_id"),
                residual_info=normalized_residual_info,
            )

    if chroma_engine.chroma_enabled():
        if saved_doc is None:
            saved_doc = await col.find_by_id(col.memories(), memory_id)
        source_doc = saved_doc if saved_doc is not None else doc
        topic_id = source_doc.get("topic_id")
        topic_id_str = str(topic_id) if topic_id is not None else ""

        created_at = source_doc.get("created_at")
        created_at_date = ""
        created_at_ts = 0
        if isinstance(created_at, datetime):
            if created_at.tzinfo is None:
                created_at = created_at.replace(tzinfo=UTC)
            created_at_date = created_at.strftime("%Y-%m-%d")
            created_at_ts = int(created_at.timestamp())

        await chroma_engine.upsert_memory_vector(
            memory_id=str(memory_id),
            content=str(source_doc.get("content", content)),
            category=str(source_doc.get("category", category)),
            importance=int(source_doc.get("importance", importance) or 0),
            topic_id=topic_id_str,
            metadata={
                "created_at_date": created_at_date,
                "created_at_ts": created_at_ts,
            },
        )

    return memory_id


async def mark_compacted_sources(
    *,
    source_ids: list[ObjectId],
    digest_id: ObjectId,
) -> dict[str, int]:
    """Record compaction-source state on memories and digests."""
    if not source_ids:
        return {"memories_updated": 0, "digests_updated": 0, "total_updated": 0}

    memories_result = await col.memories().update_many(
        {"_id": {"$in": source_ids}},
        {"$addToSet": {"compressed_from": digest_id}},
    )
    digests_result = await col.digests().update_many(
        {"_id": {"$in": source_ids}},
        {"$set": {"compacted_to": digest_id}},
    )

    memories_updated = int(memories_result.modified_count)
    digests_updated = int(digests_result.modified_count)
    return {
        "memories_updated": memories_updated,
        "digests_updated": digests_updated,
        "total_updated": memories_updated + digests_updated,
    }


async def upsert_digest_record(
    *,
    digest_type: str,
    period: str,
    content: str,
    source_ids: list[ObjectId] | None = None,
    topic_id: str = "",
) -> ObjectId:
    """Upsert a digest record and return its _id."""
    query = {"type": digest_type, "period": period}
    existing = await col.digests().find_one(query, projection={"_id": 1})

    update_doc = {
        "type": digest_type,
        "period": period,
        "content": content,
        "source_ids": source_ids or [],
    }
    if topic_id:
        update_doc["topic_id"] = topic_id

    if existing:
        await col.update_one(col.digests(), existing["_id"], {"$set": update_doc})
        return existing["_id"]

    return await col.insert_one(col.digests(), update_doc)
