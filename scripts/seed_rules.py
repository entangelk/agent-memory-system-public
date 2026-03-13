"""
Seed six default automation rules into the rules collection.
"""
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.db.connection import get_db, close_connection
from src.db.collections import now

RULES = [
    {
        "rule_key": "daily_digest_auto_create",
        "rule_type": "compaction",
        "description": "Automatically create a daily digest during daily L1 compaction",
        "legacy_descriptions": ["매일 L1 컴팩팅 시 daily digest 자동 생성"],
        "trigger": "cron:daily",
        "conditions": {},
        "actions": {"target_collection": "digests", "type": "daily"},
        "enabled": True,
    },
    {
        "rule_key": "topic_auto_create_from_clustered_memories",
        "rule_type": "topic_generation",
        "description": "Automatically create a topic document when three or more related memories accumulate",
        "legacy_descriptions": ["동일 주제 기억 3개+ 축적 시 topic 문서 자동 생성"],
        "trigger": None,
        "conditions": {"min_memory_count": 3, "similarity_threshold": 0.7},
        "actions": {"create_topic": True, "consolidate_existing": True},
        "enabled": True,
    },
    {
        "rule_key": "project_topic_from_repeated_work_mentions",
        "rule_type": "topic_generation",
        "description": "Create a project topic when the same work item is mentioned repeatedly",
        "legacy_descriptions": ["특정 작업이 반복 언급되면 project topic 생성"],
        "trigger": None,
        "conditions": {"min_mention_count": 5, "type": "project"},
        "actions": {"create_topic": True, "status": "active"},
        "enabled": True,
    },
    {
        "rule_key": "profile_refresh_on_l3_compaction",
        "rule_type": "profile_update",
        "description": "Refresh the user profile automatically during L3 compaction",
        "legacy_descriptions": ["L3 컴팩팅 시 사용자 프로필 자동 갱신"],
        "trigger": "cron:monthly",
        "conditions": {},
        "actions": {"target_collection": "profiles"},
        "enabled": True,
    },
    {
        "rule_key": "archive_inactive_topics",
        "rule_type": "archival",
        "description": "Move topics with no updates or recalls for 180 days to archived status",
        "legacy_descriptions": ["180일간 갱신/참조 없는 topic → archived 상태로 전환"],
        "trigger": None,
        "conditions": {"inactive_days": 180},
        "actions": {"set_status": "archived"},
        "enabled": True,
    },
    {
        "rule_key": "match_new_memory_to_existing_topics",
        "rule_type": "classification",
        "description": "Match new memory entities and keywords against existing topics",
        "legacy_descriptions": ["새 기억의 엔티티/키워드를 기존 topic과 매칭"],
        "trigger": None,
        "conditions": {},
        "actions": {
            "match_existing_topic": True,
            "create_new_if_threshold": True,
        },
        "enabled": True,
    },
]


async def main() -> None:
    db = get_db()
    rules_col = db["rules"]

    inserted = 0
    updated = 0
    for rule in RULES:
        payload = {k: v for k, v in rule.items() if k != "legacy_descriptions"}
        description_candidates = [rule["description"], *rule.get("legacy_descriptions", [])]
        exists = await rules_col.find_one({
            "$or": [
                {"rule_key": payload["rule_key"]},
                {
                    "rule_type": payload["rule_type"],
                    "description": {"$in": description_candidates},
                },
            ],
        })
        if not exists:
            payload["created_at"] = now()
            payload["updated_at"] = now()
            await rules_col.insert_one(payload)
            inserted += 1
            print(f"  inserted: {payload['description']}")
        else:
            await rules_col.update_one(
                {"_id": exists["_id"]},
                {"$set": {**payload, "updated_at": now()}},
            )
            updated += 1
            print(f"  synced:   {payload['description']}")

    print(f"\nInserted {inserted} rule(s), synchronized {updated} existing rule(s).")
    await close_connection()


if __name__ == "__main__":
    asyncio.run(main())
