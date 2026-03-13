"""
Residual info normalization and topic mapping index sync.
"""
from __future__ import annotations

import hashlib
from bson import ObjectId
from src.db import collections as col


_KEY_ALIASES = {
    "loc": "location",
    "place": "location",
    "region": "location",
    "addr": "location",
    "menu": "food",
    "dish": "food",
    "why": "reason",
    "cause": "reason",
    "lang": "language",
    "frameworks": "framework",
    "fw": "framework",
    "lib": "library",
    "db": "database",
}


def _normalize_key(value: object) -> str:
    if not isinstance(value, str):
        return ""
    key = value.strip().lower().replace(" ", "_").replace("-", "_")
    if not key:
        return ""
    return _KEY_ALIASES.get(key, key)


def _normalize_scalar(value: object) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        return value.strip()
    return ""


def _normalize_values(value: object) -> list[str]:
    raw_values: list[object]
    if isinstance(value, (list, tuple, set)):
        raw_values = list(value)
    else:
        raw_values = [value]

    normalized: list[str] = []
    seen: set[str] = set()
    for item in raw_values:
        scalar = _normalize_scalar(item)
        if not scalar:
            continue
        dedupe_key = scalar.lower()
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        normalized.append(scalar)
    return normalized


def normalize_residual_info(raw: dict | None) -> dict:
    if not isinstance(raw, dict):
        return {}

    merged: dict[str, list[str]] = {}
    for raw_key, raw_value in raw.items():
        key = _normalize_key(raw_key)
        if not key:
            continue
        values = _normalize_values(raw_value)
        if not values:
            continue
        current = merged.setdefault(key, [])
        seen = {v.lower() for v in current}
        for value in values:
            if value.lower() in seen:
                continue
            current.append(value)
            seen.add(value.lower())

    result: dict = {}
    for key, values in merged.items():
        if len(values) == 1:
            result[key] = values[0]
        else:
            result[key] = values
    return result


def residual_slots(residual_info: dict | None) -> list[str]:
    if not isinstance(residual_info, dict):
        return []
    return sorted([k for k in residual_info.keys() if isinstance(k, str) and k])


def _iter_pairs(residual_info: dict) -> list[tuple[str, str, str]]:
    pairs: list[tuple[str, str, str]] = []
    for slot, raw_value in residual_info.items():
        if isinstance(raw_value, list):
            values = raw_value
        else:
            values = [raw_value]

        for value in values:
            if not isinstance(value, str):
                continue
            normalized_value = value.strip().lower()
            if not normalized_value:
                continue
            value_hash = hashlib.sha1(normalized_value.encode("utf-8")).hexdigest()
            pairs.append((slot, normalized_value, value_hash))
    return pairs


def _to_object_id(topic_id: ObjectId | str | None) -> ObjectId | None:
    if isinstance(topic_id, ObjectId):
        return topic_id
    if isinstance(topic_id, str):
        try:
            return ObjectId(topic_id)
        except Exception:
            return None
    return None


async def sync_memory_residual_mappings(
    *,
    memory_id: ObjectId,
    topic_id: ObjectId | str | None,
    residual_info: dict | None,
) -> None:
    topic_oid = _to_object_id(topic_id)
    if topic_oid is None:
        return

    normalized = normalize_residual_info(residual_info)
    new_pairs = _iter_pairs(normalized)
    new_pair_keys = {(slot, value_hash) for slot, _, value_hash in new_pairs}

    mapping_col = col.topic_residual_mappings()
    existing = await mapping_col.find(
        {"topic_id": topic_oid, "memory_ids": memory_id},
        projection={"slot": 1, "value_hash": 1},
    ).to_list(length=500)
    existing_pair_keys = {(doc.get("slot", ""), doc.get("value_hash", "")) for doc in existing}

    stale_pairs = existing_pair_keys - new_pair_keys
    for slot, value_hash in stale_pairs:
        await mapping_col.update_one(
            {"topic_id": topic_oid, "slot": slot, "value_hash": value_hash},
            {"$pull": {"memory_ids": memory_id}, "$set": {"updated_at": col.now()}},
        )

    await mapping_col.delete_many({"topic_id": topic_oid, "memory_ids": {"$size": 0}})

    now = col.now()
    for slot, normalized_value, value_hash in new_pairs:
        await mapping_col.update_one(
            {"topic_id": topic_oid, "slot": slot, "value_hash": value_hash},
            {
                "$setOnInsert": {
                    "topic_id": topic_oid,
                    "slot": slot,
                    "value_hash": value_hash,
                    "value": normalized_value,
                    "created_at": now,
                },
                "$set": {"updated_at": now},
                "$addToSet": {"memory_ids": memory_id},
            },
            upsert=True,
        )
