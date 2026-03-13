"""
Rebuild the Chroma cache from the MongoDB source of truth.

Default behavior:
- re-embed and upsert both topics (centroids) and memories
- remove existing Chroma `type=centroid` and `type=memory` records before reloading

Usage:
  python scripts/rebuild_chroma.py
  python scripts/rebuild_chroma.py --batch-size 100
  python scripts/rebuild_chroma.py --collection-name memory_bge_m3_v2
  python scripts/rebuild_chroma.py --no-clear --topics-only
"""
import argparse
import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src import config
from src.db import collections as col
from src.db.connection import close_connection
from src.engine import chroma_engine


def _clean_text(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return value.strip()


async def _rebuild_topics(limit: int, batch_size: int) -> dict:
    scanned = 0
    indexed = 0
    failed = 0
    skipped_empty = 0
    queued_rows: list[dict] = []

    async def flush_batch() -> None:
        nonlocal indexed, failed
        if not queued_rows:
            return
        ok_count, fail_count = await chroma_engine.upsert_topic_centroids_batch(queued_rows)
        indexed += ok_count
        failed += fail_count
        queued_rows.clear()

    query = {"type": "centroid", "status": {"$ne": "archived"}}
    cursor = col.topics().find(
        query,
        projection={
            "title": 1,
            "summary": 1,
            "memory_count": 1,
            "centroid_version": 1,
            "centroid_updated_at": 1,
        },
        sort=[("updated_at", -1)],
    )
    if limit > 0:
        cursor = cursor.limit(limit)

    async for doc in cursor:
        scanned += 1
        topic_id = str(doc["_id"])
        title = _clean_text(doc.get("title", ""))
        summary = _clean_text(doc.get("summary", ""))
        if not title and not summary:
            skipped_empty += 1
            continue

        metadata = {
            "centroid_version": int(doc.get("centroid_version", 0) or 0),
        }
        centroid_updated_at = doc.get("centroid_updated_at")
        if centroid_updated_at is not None:
            metadata["centroid_updated_at"] = str(centroid_updated_at)

        queued_rows.append({
            "topic_id": topic_id,
            "title": title,
            "summary": summary,
            "memory_count": int(doc.get("memory_count", 0) or 0),
            "metadata": metadata,
        })
        if len(queued_rows) >= max(1, int(batch_size)):
            await flush_batch()

    await flush_batch()

    return {
        "query": query,
        "scanned": scanned,
        "indexed": indexed,
        "failed": failed,
        "skipped_empty": skipped_empty,
    }


async def _rebuild_memories(limit: int) -> dict:
    scanned = 0
    indexed = 0
    failed = 0
    skipped_empty = 0

    query = {}
    cursor = col.memories().find(
        query,
        projection={
            "content": 1,
            "category": 1,
            "importance": 1,
            "topic_id": 1,
            "created_at": 1,
            "updated_at": 1,
        },
        sort=[("updated_at", -1)],
    )
    if limit > 0:
        cursor = cursor.limit(limit)

    async for doc in cursor:
        scanned += 1
        memory_id = str(doc["_id"])
        content = _clean_text(doc.get("content", ""))
        if not content:
            skipped_empty += 1
            continue

        topic_id = doc.get("topic_id")
        topic_id_str = str(topic_id) if topic_id is not None else ""
        created_at = doc.get("created_at")
        created_at_date = ""
        created_at_ts = 0
        if created_at is not None:
            try:
                created_at_date = created_at.strftime("%Y-%m-%d")
                created_at_ts = int(created_at.timestamp())
            except Exception:
                created_at_date = ""
                created_at_ts = 0
        vector_id = await chroma_engine.upsert_memory_vector(
            memory_id=memory_id,
            content=content,
            category=str(doc.get("category", "") or ""),
            importance=int(doc.get("importance", 0) or 0),
            topic_id=topic_id_str,
            metadata={
                "created_at_date": created_at_date,
                "created_at_ts": created_at_ts,
                "updated_at": str(doc.get("updated_at")) if doc.get("updated_at") else "",
            },
        )
        if vector_id:
            indexed += 1
        else:
            failed += 1

    return {
        "query": query,
        "scanned": scanned,
        "indexed": indexed,
        "failed": failed,
        "skipped_empty": skipped_empty,
    }


async def main() -> None:
    parser = argparse.ArgumentParser(description="Rebuild Chroma vectors from Mongo SoT.")
    parser.add_argument(
        "--collection-name",
        type=str,
        default="",
        help="Target Chroma collection name (defaults to CHROMA_COLLECTION_NAME)",
    )
    parser.add_argument(
        "--clear",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Whether to delete existing centroid/memory vectors before reload (default: true)",
    )
    parser.add_argument("--topics-limit", type=int, default=0, help="Maximum number of topics to process (0 = all)")
    parser.add_argument("--memories-limit", type=int, default=0, help="Maximum number of memories to process (0 = all)")
    parser.add_argument("--batch-size", type=int, default=100, help="Batch upsert size for topics (default: 100)")
    parser.add_argument("--topics-only", action="store_true", help="Rebuild only topics (centroids)")
    args = parser.parse_args()

    # Operational scripts explicitly enable the Chroma path.
    config.CHROMA_ENABLED = True

    if args.collection_name.strip():
        chroma_engine.set_collection_name(args.collection_name.strip())
    elif config.CHROMA_COLLECTION_NAME:
        chroma_engine.set_collection_name(config.CHROMA_COLLECTION_NAME)

    cleared_centroids = 0
    cleared_memories = 0
    if args.clear:
        cleared_centroids = await chroma_engine.clear_topic_centroids()
        if not args.topics_only:
            cleared_memories = await chroma_engine.clear_memory_vectors()

    topics_report = await _rebuild_topics(args.topics_limit, args.batch_size)
    memories_report = {
        "query": {},
        "scanned": 0,
        "indexed": 0,
        "failed": 0,
        "skipped_empty": 0,
    }
    if not args.topics_only:
        memories_report = await _rebuild_memories(args.memories_limit)

    print(json.dumps({
        "status": "ok",
        "collection_name": config.CHROMA_COLLECTION_NAME,
        "embedding_model": config.EMBEDDING_MODEL_NAME,
        "cleared": {
            "centroids": int(cleared_centroids),
            "memories": int(cleared_memories),
            "total": int(cleared_centroids + cleared_memories),
        },
        "topics": topics_report,
        "memories": memories_report,
    }, ensure_ascii=False))
    await close_connection()


if __name__ == "__main__":
    asyncio.run(main())
