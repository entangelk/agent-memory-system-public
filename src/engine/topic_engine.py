"""
Topic auto-generation and hierarchy management helpers.

Behavior:
- auto-create a topic when three or more memories share the same entities
- archive inactive topics after 180 days by policy
"""
import hashlib
import re
from datetime import datetime, timedelta, UTC
from bson import ObjectId
from src.db import collections as col
from src.engine import chroma_engine, residual_engine

_DIGEST_TOPIC_LEVEL = {
    "daily": 1,
    "weekly": 2,
    "monthly": 3,
    "yearly": 4,
}


def normalize_topic_name(value: object) -> str:
    if not isinstance(value, str):
        return ""
    cleaned = value.strip()
    if not cleaned:
        return ""
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.lower()


def _to_aware_datetime(value: object) -> datetime | None:
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value


def _to_object_id(value: ObjectId | str | None) -> ObjectId | None:
    if isinstance(value, ObjectId):
        return value
    if isinstance(value, str):
        try:
            return ObjectId(value)
        except Exception:
            return None
    return None


def _normalize_aliases(values: list[str] | None) -> tuple[list[str], list[str]]:
    if not values:
        return [], []
    cleaned: list[str] = []
    normalized: list[str] = []
    seen_cleaned: set[str] = set()
    seen_normalized: set[str] = set()
    for value in values:
        if not isinstance(value, str):
            continue
        v = re.sub(r"\s+", " ", value.strip())
        if not v:
            continue
        key = v.lower()
        if key in seen_cleaned:
            continue
        seen_cleaned.add(key)
        cleaned.append(v)

        nv = normalize_topic_name(v)
        if nv and nv not in seen_normalized:
            seen_normalized.add(nv)
            normalized.append(nv)
    return cleaned, normalized


def _make_taxonomy_slug(name: str, level: int, parent_topic_id: ObjectId | None) -> str:
    canonical = normalize_topic_name(name)
    safe = re.sub(r"[^0-9a-zA-Z가-힣]+", "-", canonical).strip("-")
    if not safe:
        safe = "topic"
    parent_key = str(parent_topic_id) if parent_topic_id is not None else "root"
    suffix = hashlib.sha1(f"{level}:{parent_key}:{canonical}".encode("utf-8")).hexdigest()[:10]
    return f"tax-l{level}-{safe[:30]}-{suffix}"


async def maybe_create_topic(entities: list[str]) -> ObjectId | None:
    """Decide whether to auto-create a topic from entities and return the topic _id."""
    if not entities:
        return None

    # Count memories that include the same entities.
    memories_col = col.memories()
    count = await memories_col.count_documents({"entities": {"$in": entities}})

    rule = await col.rules().find_one({
        "rule_type": "topic_generation",
        "enabled": True,
        "conditions.min_memory_count": {"$lte": count},
    })
    if not rule:
        return None

    # Reuse an existing topic when the entity combination already maps to one.
    slug = _make_slug(entities)
    existing = await col.topics().find_one({"slug": slug})
    if existing:
        topic_id = existing["_id"]
        await memories_col.update_many(
            {"entities": {"$in": entities}},
            {"$set": {"topic_id": topic_id}},
        )

        title = existing.get("title", "")
        canonical = normalize_topic_name(title)
        if canonical:
            await col.topics().update_one(
                {"_id": topic_id},
                {
                    "$set": {
                        "level": 1,
                        "parent_topic_id": None,
                        "canonical_name": existing.get("canonical_name") or canonical,
                    },
                    "$addToSet": {
                        "aliases": {"$each": [title]},
                        "aliases_norm": {"$each": [canonical]},
                    },
                },
            )

        # Refresh memory_count before returning the existing topic.
        await col.update_one(
            col.topics(),
            topic_id,
            {"$set": {"memory_count": count}},
        )
        await chroma_engine.upsert_topic_centroid(
            topic_id=str(topic_id),
            title=existing.get("title", ""),
            summary=existing.get("summary", ""),
            memory_count=count,
        )
        await _sync_residual_mappings_for_topic(topic_id=topic_id, entities=entities)
        return topic_id

    # Create a new topic when no existing match is found.
    topic_doc = {
        "slug": slug,
        "title": _make_title(entities),
        "type": "centroid",
        "status": "active",
        "summary": "",
        "sections": {},
        "linked_memory_ids": [],
        "linked_triple_ids": [],
        "category_id": None,
        "memory_count": count,
        "auto_generated": True,
        "level": 1,
        "parent_topic_id": None,
        "canonical_name": normalize_topic_name(_make_title(entities)),
        "aliases": [],
        "aliases_norm": [],
        "centroid_version": 0,
        "centroid_updated_at": datetime.now(UTC),
        "centroid_source_digest_ids": [],
    }
    topic_id = await col.insert_one(col.topics(), topic_doc)

    # Link matching memories back to the new topic.
    await memories_col.update_many(
        {"entities": {"$in": entities}},
        {"$set": {"topic_id": topic_id}},
    )
    await _sync_residual_mappings_for_topic(topic_id=topic_id, entities=entities)

    await chroma_engine.upsert_topic_centroid(
        topic_id=str(topic_id),
        title=topic_doc.get("title", ""),
        summary=topic_doc.get("summary", ""),
        memory_count=count,
    )

    return topic_id


async def archive_inactive_topics(inactive_days: int = 180) -> int:
    """Archive inactive topics and return the number of updated documents."""
    cutoff = datetime.now(UTC) - timedelta(days=inactive_days)
    result = await col.topics().update_many(
        {
            "status": "active",
            "updated_at": {"$lt": cutoff},
        },
        {"$set": {"status": "archived"}},
    )
    return result.modified_count


def _make_slug(entities: list[str]) -> str:
    normalized = [e.lower().replace(" ", "-") for e in sorted(entities)]
    return "-".join(normalized[:3])  # Use at most three entities in the slug.


def _make_title(entities: list[str]) -> str:
    return " / ".join(entities[:3])


async def update_centroid_summary(
    topic_id: str,
    summary: str,
    source_digest_ids: list[str] | None = None,
) -> bool:
    """Update a topic centroid summary and synchronize it to Chroma."""
    if not summary.strip():
        return False

    try:
        oid = ObjectId(topic_id)
    except Exception:
        return False

    topic = await col.find_by_id(col.topics(), oid)
    if not topic:
        return False

    now = datetime.now(UTC)
    current_version = int(topic.get("centroid_version", 0) or 0)
    next_version = current_version + 1
    set_fields: dict = {
        "summary": summary,
        "type": "centroid",
        "status": "active",
        "centroid_version": next_version,
        "centroid_updated_at": now,
    }
    if source_digest_ids is not None:
        normalized_ids: list[str] = []
        seen: set[str] = set()
        for raw in source_digest_ids:
            if not isinstance(raw, str):
                continue
            v = raw.strip()
            if not v or v in seen:
                continue
            normalized_ids.append(v)
            seen.add(v)
        set_fields["centroid_source_digest_ids"] = normalized_ids

    await col.update_one(
        col.topics(),
        oid,
        {"$set": set_fields},
    )
    await chroma_engine.upsert_topic_centroid(
        topic_id=topic_id,
        title=topic.get("title", ""),
        summary=summary,
        memory_count=topic.get("memory_count", 0),
        metadata={
            "centroid_version": next_version,
            "centroid_updated_at": now.isoformat(),
        },
    )
    return True


async def infer_dominant_topic_id_from_memories(memory_ids: list[ObjectId]) -> str | None:
    """Return the most common topic_id referenced by the given memories."""
    if not memory_ids:
        return None

    cursor = col.memories().find(
        {"_id": {"$in": memory_ids}, "topic_id": {"$ne": None}},
        projection={"topic_id": 1},
    )
    docs = await cursor.to_list(length=len(memory_ids))

    counts: dict[str, int] = {}
    for doc in docs:
        topic_id = doc.get("topic_id")
        if topic_id is None:
            continue
        key = str(topic_id)
        counts[key] = counts.get(key, 0) + 1

    if not counts:
        return None
    return max(counts.items(), key=lambda item: item[1])[0]


async def upsert_topic_node(
    *,
    name: str,
    level: int,
    parent_topic_id: ObjectId | str | None = None,
    aliases: list[str] | None = None,
) -> ObjectId | None:
    title = re.sub(r"\s+", " ", name.strip()) if isinstance(name, str) else ""
    canonical = normalize_topic_name(title)
    if not title or not canonical or level < 1:
        return None

    parent_oid = _to_object_id(parent_topic_id)
    query = {
        "level": level,
        "parent_topic_id": parent_oid,
        "$or": [
            {"canonical_name": canonical},
            {"aliases_norm": canonical},
        ],
    }
    existing = await col.topics().find_one(query, projection={"_id": 1, "canonical_name": 1})
    alias_values, alias_norm_values = _normalize_aliases((aliases or []) + [title])

    if existing:
        update: dict = {"$set": {}, "$addToSet": {}}
        if not existing.get("canonical_name"):
            update["$set"]["canonical_name"] = canonical
        if alias_values:
            update["$addToSet"]["aliases"] = {"$each": alias_values}
        if alias_norm_values:
            update["$addToSet"]["aliases_norm"] = {"$each": alias_norm_values}

        if not update["$set"]:
            del update["$set"]
        if not update["$addToSet"]:
            del update["$addToSet"]
        if update:
            await col.update_one(col.topics(), existing["_id"], update)
        return existing["_id"]

    topic_doc = {
        "slug": _make_taxonomy_slug(title, level, parent_oid),
        "title": title,
        "type": "centroid" if level == 1 else "taxonomy",
        "status": "active",
        "summary": "",
        "sections": {},
        "linked_memory_ids": [],
        "linked_triple_ids": [],
        "category_id": None,
        "memory_count": 0,
        "auto_generated": True,
        "level": level,
        "parent_topic_id": parent_oid,
        "canonical_name": canonical,
        "aliases": alias_values,
        "aliases_norm": alias_norm_values,
    }
    if level == 1:
        topic_doc["centroid_version"] = 0
        topic_doc["centroid_updated_at"] = datetime.now(UTC)
        topic_doc["centroid_source_digest_ids"] = []
    return await col.insert_one(col.topics(), topic_doc)


async def ensure_topic_path(
    *,
    topic_path: list[str],
    base_topic_id: ObjectId | str | None = None,
    base_aliases: list[str] | None = None,
) -> list[dict]:
    names = []
    for raw in topic_path:
        if not isinstance(raw, str):
            continue
        cleaned = re.sub(r"\s+", " ", raw.strip())
        if cleaned:
            names.append(cleaned)
    if not names:
        return []

    nodes: list[dict] = []
    parent_oid: ObjectId | None = None

    base_oid = _to_object_id(base_topic_id)
    if base_oid is not None:
        base_doc = await col.find_by_id(col.topics(), base_oid)
        if base_doc:
            base_name = names[0]
            canonical = normalize_topic_name(base_name)
            alias_values, alias_norm_values = _normalize_aliases((base_aliases or []) + [base_name])
            update: dict = {
                "$set": {
                    "level": 1,
                    "parent_topic_id": None,
                },
                "$addToSet": {},
            }
            if not base_doc.get("canonical_name") and canonical:
                update["$set"]["canonical_name"] = canonical
            if alias_values:
                update["$addToSet"]["aliases"] = {"$each": alias_values}
            if alias_norm_values:
                update["$addToSet"]["aliases_norm"] = {"$each": alias_norm_values}
            if not update["$addToSet"]:
                del update["$addToSet"]
            await col.update_one(col.topics(), base_oid, update)

            nodes.append({
                "level": 1,
                "topic_id": str(base_oid),
                "title": base_doc.get("title", base_name),
            })
            parent_oid = base_oid
            names = names[1:]

    for name in names:
        level = len(nodes) + 1
        oid = await upsert_topic_node(
            name=name,
            level=level,
            parent_topic_id=parent_oid,
            aliases=base_aliases if level == 1 else None,
        )
        if oid is None:
            continue
        doc = await col.find_by_id(col.topics(), oid)
        nodes.append({
            "level": level,
            "topic_id": str(oid),
            "title": doc.get("title", name) if doc else name,
        })
        parent_oid = oid

    return nodes


def select_topic_id_for_digest_type(topic_nodes: list[dict], digest_type: str) -> str | None:
    if not topic_nodes:
        return None
    target_level = _DIGEST_TOPIC_LEVEL.get((digest_type or "").strip().lower())
    if target_level is None:
        return topic_nodes[-1].get("topic_id")
    for node in topic_nodes:
        if int(node.get("level", 0)) == target_level:
            return node.get("topic_id")
    return topic_nodes[-1].get("topic_id")


async def search_topics(
    *,
    query: str,
    level: int | None = None,
    top_k: int = 10,
) -> list[dict]:
    text = query.strip() if isinstance(query, str) else ""
    if not text:
        return []

    normalized = normalize_topic_name(text)
    escaped_text = re.escape(text)
    escaped_norm = re.escape(normalized) if normalized else escaped_text
    mongo_filter: dict = {
        "$or": [
            {"title": {"$regex": escaped_text, "$options": "i"}},
            {"canonical_name": {"$regex": escaped_norm, "$options": "i"}},
            {"aliases": {"$regex": escaped_text, "$options": "i"}},
            {"aliases_norm": {"$regex": escaped_norm, "$options": "i"}},
        ],
    }
    if isinstance(level, int) and level > 0:
        mongo_filter["level"] = level

    docs = await col.topics().find(
        mongo_filter,
        projection={
            "title": 1,
            "canonical_name": 1,
            "aliases": 1,
            "level": 1,
            "parent_topic_id": 1,
            "memory_count": 1,
            "status": 1,
            "type": 1,
            "slug": 1,
        },
        sort=[("memory_count", -1), ("updated_at", -1)],
        limit=max(1, min(int(top_k), 50)),
    ).to_list(length=max(1, min(int(top_k), 50)))

    topic_ids = [doc["_id"] for doc in docs]
    child_count_map: dict[ObjectId, int] = {}
    if topic_ids:
        grouped = await col.topics().aggregate([
            {"$match": {"parent_topic_id": {"$in": topic_ids}}},
            {"$group": {"_id": "$parent_topic_id", "count": {"$sum": 1}}},
        ]).to_list(length=len(topic_ids))
        for row in grouped:
            parent_id = row.get("_id")
            if isinstance(parent_id, ObjectId):
                child_count_map[parent_id] = int(row.get("count", 0))

    result: list[dict] = []
    for doc in docs:
        parent = doc.get("parent_topic_id")
        result.append({
            "id": str(doc["_id"]),
            "slug": doc.get("slug", ""),
            "title": doc.get("title", ""),
            "canonical_name": doc.get("canonical_name", ""),
            "aliases": doc.get("aliases", []),
            "level": int(doc.get("level", 1) or 1),
            "parent_topic_id": str(parent) if isinstance(parent, ObjectId) else None,
            "memory_count": int(doc.get("memory_count", 0) or 0),
            "children_count": child_count_map.get(doc["_id"], 0),
            "status": doc.get("status", ""),
            "type": doc.get("type", ""),
        })
    return result


async def get_topic_detail(topic_id: str, include_children: bool = False) -> dict | None:
    oid = _to_object_id(topic_id)
    if oid is None:
        return None

    doc = await col.topics().find_one(
        {"_id": oid},
        projection={
            "title": 1,
            "canonical_name": 1,
            "aliases": 1,
            "level": 1,
            "parent_topic_id": 1,
            "memory_count": 1,
            "status": 1,
            "type": 1,
            "slug": 1,
        },
    )
    if not doc:
        return None

    parent = doc.get("parent_topic_id")
    result = {
        "id": str(doc["_id"]),
        "slug": doc.get("slug", ""),
        "title": doc.get("title", ""),
        "canonical_name": doc.get("canonical_name", ""),
        "aliases": doc.get("aliases", []),
        "level": int(doc.get("level", 1) or 1),
        "parent_topic_id": str(parent) if isinstance(parent, ObjectId) else None,
        "memory_count": int(doc.get("memory_count", 0) or 0),
        "status": doc.get("status", ""),
        "type": doc.get("type", ""),
    }

    if include_children:
        children = await col.topics().find(
            {"parent_topic_id": oid},
            projection={"title": 1, "level": 1, "slug": 1, "memory_count": 1},
            sort=[("memory_count", -1), ("updated_at", -1)],
            limit=30,
        ).to_list(length=30)
        result["children"] = [{
            "id": str(child["_id"]),
            "slug": child.get("slug", ""),
            "title": child.get("title", ""),
            "level": int(child.get("level", 1) or 1),
            "memory_count": int(child.get("memory_count", 0) or 0),
        } for child in children]

    return result


async def centroid_refresh_recommendations(
    *,
    topic_ids: list[str],
    stale_days: int,
) -> list[dict]:
    """Return per-topic centroid freshness checks and refresh recommendations."""
    object_ids: list[ObjectId] = []
    for raw in topic_ids:
        oid = _to_object_id(raw)
        if oid is not None:
            object_ids.append(oid)
    if not object_ids:
        return []

    topics = await col.topics().find(
        {"_id": {"$in": object_ids}},
        projection={
            "title": 1,
            "centroid_updated_at": 1,
            "centroid_version": 1,
            "updated_at": 1,
        },
    ).to_list(length=len(object_ids))
    if not topics:
        return []

    latest_digest_rows = await col.digests().aggregate([
        {"$match": {"topic_id": {"$in": object_ids}}},
        {"$project": {"topic_id": 1, "ts": {"$ifNull": ["$updated_at", "$created_at"]}}},
        {"$group": {"_id": "$topic_id", "latest_digest_at": {"$max": "$ts"}}},
    ]).to_list(length=len(object_ids))
    digest_map: dict[ObjectId, datetime] = {}
    for row in latest_digest_rows:
        topic_oid = row.get("_id")
        ts = _to_aware_datetime(row.get("latest_digest_at"))
        if isinstance(topic_oid, ObjectId) and ts is not None:
            digest_map[topic_oid] = ts

    now = datetime.now(UTC)
    recommendations: list[dict] = []
    for topic in topics:
        centroid_updated_at = _to_aware_datetime(topic.get("centroid_updated_at"))
        if centroid_updated_at is None:
            centroid_updated_at = _to_aware_datetime(topic.get("updated_at"))
        if centroid_updated_at is None:
            continue

        reasons: list[str] = []
        stale_by_days = (now - centroid_updated_at).days >= max(1, stale_days)
        if stale_by_days:
            reasons.append("older_than_policy")

        latest_digest_at = digest_map.get(topic["_id"])
        if latest_digest_at is not None and latest_digest_at > centroid_updated_at:
            reasons.append("new_digest_after_centroid")

        if not reasons:
            continue

        recommendations.append({
            "topic_id": str(topic["_id"]),
            "title": topic.get("title", ""),
            "centroid_version": int(topic.get("centroid_version", 0) or 0),
            "centroid_updated_at": centroid_updated_at.isoformat(),
            "latest_digest_at": latest_digest_at.isoformat() if latest_digest_at else None,
            "reasons": reasons,
        })

    recommendations.sort(key=lambda row: row.get("centroid_updated_at", ""))
    return recommendations


async def _sync_residual_mappings_for_topic(*, topic_id: ObjectId, entities: list[str]) -> None:
    docs = await col.memories().find(
        {
            "topic_id": topic_id,
            "entities": {"$in": entities},
            "residual_info": {"$exists": True},
        },
        projection={"residual_info": 1},
    ).to_list(length=500)
    for doc in docs:
        residual_info = doc.get("residual_info")
        if not isinstance(residual_info, dict):
            continue
        await residual_engine.sync_memory_residual_mappings(
            memory_id=doc["_id"],
            topic_id=topic_id,
            residual_info=residual_info,
        )
