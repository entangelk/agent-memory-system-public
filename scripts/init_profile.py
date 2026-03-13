"""
Initialize the default user profile.
Creates the "primary" profile document if it does not exist.
"""
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.db.connection import get_db, close_connection
from src.db.collections import now
from src.engine.sensitivity_engine import DEFAULT_POLICY


async def main() -> None:
    db = get_db()
    profiles_col = db["profiles"]

    existing = await profiles_col.find_one({"user_id": "primary"})
    if existing:
        print("Profile already exists. (user_id=primary)")
        await close_connection()
        return

    profile = {
        "user_id": "primary",
        "summary": "",
        "preferences": {},
        "communication_style": "",
        "last_consolidated": None,
        "sensitivity_policy": dict(DEFAULT_POLICY),
        "created_at": now(),
        "updated_at": now(),
    }
    result = await profiles_col.insert_one(profile)
    print(f"Created default profile. (id={result.inserted_id})")

    await close_connection()


if __name__ == "__main__":
    asyncio.run(main())
