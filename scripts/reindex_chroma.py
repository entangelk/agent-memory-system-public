"""
Reindex Chroma centroid vectors from Mongo topics (source of truth).

Usage:
  python scripts/reindex_chroma.py --clear
  python scripts/reindex_chroma.py --limit 100
"""
import argparse
import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.db import collections as col
from src.db.connection import close_connection
from src.engine import chroma_engine


async def main() -> None:
    parser = argparse.ArgumentParser(description="Reindex Chroma centroid vectors from Mongo topics.")
    parser.add_argument("--clear", action="store_true", help="Delete existing Chroma centroid records before reindexing.")
    parser.add_argument("--limit", type=int, default=0, help="Maximum number of topics to process (0 = all)")
    args = parser.parse_args()

    if not chroma_engine.chroma_enabled():
        print(json.dumps({
            "status": "skipped",
            "reason": "chroma_disabled",
            "cleared": 0,
            "scanned": 0,
            "indexed": 0,
            "failed": 0,
            "skipped_empty": 0,
        }, ensure_ascii=False))
        await close_connection()
        return

    deleted = 0
    if args.clear:
        deleted = await chroma_engine.clear_topic_centroids()

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
    if args.limit > 0:
        cursor = cursor.limit(args.limit)

    max_len = args.limit if args.limit > 0 else 5000
    docs = await cursor.to_list(length=max_len)

    indexed = 0
    failed = 0
    skipped_empty = 0
    for doc in docs:
        topic_id = str(doc["_id"])
        summary = str(doc.get("summary", "") or "").strip()
        title = str(doc.get("title", "") or "").strip()
        if not summary and not title:
            skipped_empty += 1
            continue

        metadata = {
            "centroid_version": int(doc.get("centroid_version", 0) or 0),
        }
        centroid_updated_at = doc.get("centroid_updated_at")
        if centroid_updated_at is not None:
            metadata["centroid_updated_at"] = str(centroid_updated_at)

        vector_id = await chroma_engine.upsert_topic_centroid(
            topic_id=topic_id,
            title=title,
            summary=summary,
            memory_count=doc.get("memory_count", 0),
            metadata=metadata,
        )
        if vector_id:
            indexed += 1
        else:
            failed += 1

    print(json.dumps({
        "status": "ok",
        "cleared": int(deleted),
        "scanned": len(docs),
        "indexed": indexed,
        "failed": failed,
        "skipped_empty": skipped_empty,
        "query": query,
    }, ensure_ascii=False))
    await close_connection()


if __name__ == "__main__":
    asyncio.run(main())
