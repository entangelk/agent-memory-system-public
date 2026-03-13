"""
Initialize MongoDB collections and indexes.
"""
import asyncio
import sys
from pathlib import Path

# Add the project root to sys.path.
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.db.connection import get_db, close_connection
from src.db.indexes import create_all_indexes

COLLECTIONS = [
    "memories",
    "triples",
    "sessions",
    "profiles",
    "topics",
    "digests",
    "rules",
    "categories",
    "pending_memories",
    "topic_residual_mappings",
]


async def main() -> None:
    db = get_db()

    # Create collections if they do not exist yet.
    existing = set(await db.list_collection_names())
    for name in COLLECTIONS:
        if name not in existing:
            await db.create_collection(name)
            print(f"  created collection: {name}")
        else:
            print(f"  exists collection:  {name}")

    # Create indexes.
    await create_all_indexes()
    print("\nIndexes created successfully.")

    await close_connection()
    print("Done.")


if __name__ == "__main__":
    asyncio.run(main())
