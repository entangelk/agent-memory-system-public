import pymongo
from src.db.connection import get_db


async def create_all_indexes() -> None:
    db = get_db()

    # memories
    await db["memories"].create_index([("entities", pymongo.ASCENDING), ("category", pymongo.ASCENDING)])
    await db["memories"].create_index([("memory_tier", pymongo.ASCENDING), ("retention_score", pymongo.DESCENDING)])
    await db["memories"].create_index("topic_id", sparse=True)
    await db["memories"].create_index([("$**", pymongo.TEXT)], name="memories_text")

    # triples
    await db["triples"].create_index([("subject", pymongo.ASCENDING), ("predicate", pymongo.ASCENDING)])
    await db["triples"].create_index([("object", pymongo.ASCENDING), ("predicate", pymongo.ASCENDING)])

    # sessions
    await db["sessions"].create_index("started_at")

    # profiles
    await db["profiles"].create_index("user_id", unique=True)

    # topics
    await db["topics"].create_index("slug", unique=True)
    await db["topics"].create_index("category_id", sparse=True)
    await db["topics"].create_index("status")
    await db["topics"].create_index([("level", pymongo.ASCENDING), ("parent_topic_id", pymongo.ASCENDING), ("canonical_name", pymongo.ASCENDING)])
    await db["topics"].create_index("aliases_norm")
    await db["topics"].create_index("parent_topic_id", sparse=True)

    # digests
    await db["digests"].create_index([("type", pymongo.ASCENDING), ("period", pymongo.ASCENDING)], unique=True)

    # rules
    await db["rules"].create_index("rule_type")

    # categories
    await db["categories"].create_index("slug", unique=True)
    await db["categories"].create_index("parent_id", sparse=True)

    # pending_memories
    await db["pending_memories"].create_index("status")
    await db["pending_memories"].create_index("source_session", sparse=True)

    # topic_residual_mappings
    await db["topic_residual_mappings"].create_index(
        [("topic_id", pymongo.ASCENDING), ("slot", pymongo.ASCENDING), ("value_hash", pymongo.ASCENDING)],
        unique=True,
    )
    await db["topic_residual_mappings"].create_index("memory_ids")
    await db["topic_residual_mappings"].create_index([("topic_id", pymongo.ASCENDING), ("slot", pymongo.ASCENDING)])
