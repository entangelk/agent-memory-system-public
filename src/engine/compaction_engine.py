"""
Compaction status aggregation engine.
The server remains a pure data layer with no LLM calls.
It only counts uncompacted items and returns raw sources.
"""
from datetime import datetime, timedelta, UTC
from src.db import collections as col

THRESHOLDS = {
    "L1_daily": {"source": "memories", "digest_type": "daily", "threshold": 3},
    "L2_weekly": {"source": "daily", "digest_type": "weekly", "threshold": 5},
    "L3_monthly": {"source": "weekly", "digest_type": "monthly", "threshold": 3},
    "L4_yearly": {"source": "monthly", "digest_type": "yearly", "threshold": 6},
}


def _previous_period_filter(level: str) -> dict:
    """Return the previous-period time filter for a compaction level."""
    now = datetime.now(UTC)
    if level == "L1_daily":
        # Yesterday 00:00 through today 00:00.
        today = now.replace(hour=0, minute=0, second=0, microsecond=0)
        return {"created_at": {"$gte": today - timedelta(days=1), "$lt": today}}
    elif level == "L2_weekly":
        # Previous week (7-14 days ago).
        week_ago = now - timedelta(days=7)
        two_weeks_ago = now - timedelta(days=14)
        return {"created_at": {"$gte": two_weeks_ago, "$lt": week_ago}}
    elif level == "L3_monthly":
        # Previous month.
        first_of_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        first_of_prev = (first_of_month - timedelta(days=1)).replace(day=1)
        return {"created_at": {"$gte": first_of_prev, "$lt": first_of_month}}
    elif level == "L4_yearly":
        # Previous year.
        first_of_year = now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        first_of_prev_year = first_of_year.replace(year=first_of_year.year - 1)
        return {"created_at": {"$gte": first_of_prev_year, "$lt": first_of_year}}
    return {}


async def _count_uncompacted(level: str) -> int:
    """Count uncompacted items for the requested level."""
    config = THRESHOLDS[level]
    time_filter = _previous_period_filter(level)

    if level == "L1_daily":
        # Memories that have not been compacted into a digest yet.
        query = {**time_filter, "compressed_from": {"$size": 0}}
        return await col.memories().count_documents(query)
    else:
        # Lower-level digests that have not been compacted upward yet.
        query = {
            "type": config["source"],
            **time_filter,
            "compacted_to": {"$exists": False},
        }
        return await col.digests().count_documents(query)


async def get_compaction_status() -> dict:
    """Return compaction status for all levels."""
    status = {}
    for level, config in THRESHOLDS.items():
        pending = await _count_uncompacted(level)
        status[level] = {
            "pending_count": pending,
            "threshold": config["threshold"],
            "ready": pending >= config["threshold"],
        }
    return status


async def get_compaction_hint() -> dict:
    """Return the compaction hint payload for memory_save responses."""
    status = await get_compaction_status()
    return {k: v for k, v in status.items() if v["ready"]}


async def fetch_compaction_sources(level: str) -> list[dict]:
    """Return uncompacted source records for the memory_compact tool."""
    if level not in THRESHOLDS:
        raise ValueError(f"Unknown compaction level: {level}")

    config = THRESHOLDS[level]
    time_filter = _previous_period_filter(level)

    if level == "L1_daily":
        query = {**time_filter, "compressed_from": {"$size": 0}}
        cursor = col.memories().find(query, sort=[("created_at", 1)])
        docs = await cursor.to_list(length=200)
    else:
        query = {
            "type": config["source"],
            **time_filter,
            "compacted_to": {"$exists": False},
        }
        cursor = col.digests().find(query, sort=[("created_at", 1)])
        docs = await cursor.to_list(length=200)

    # Convert ObjectIds to strings for tool responses.
    for doc in docs:
        doc["_id"] = str(doc["_id"])
    return docs
