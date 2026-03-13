from datetime import datetime, UTC
from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorCollection
from src.db.connection import get_db


def col(name: str) -> AsyncIOMotorCollection:
    return get_db()[name]


def memories() -> AsyncIOMotorCollection:
    return col("memories")


def triples() -> AsyncIOMotorCollection:
    return col("triples")


def sessions() -> AsyncIOMotorCollection:
    return col("sessions")


def profiles() -> AsyncIOMotorCollection:
    return col("profiles")


def topics() -> AsyncIOMotorCollection:
    return col("topics")


def digests() -> AsyncIOMotorCollection:
    return col("digests")


def rules() -> AsyncIOMotorCollection:
    return col("rules")


def categories() -> AsyncIOMotorCollection:
    return col("categories")


def pending_memories() -> AsyncIOMotorCollection:
    return col("pending_memories")


def topic_residual_mappings() -> AsyncIOMotorCollection:
    return col("topic_residual_mappings")


# --- Basic CRUD helpers ---

def now() -> datetime:
    return datetime.now(UTC)


async def insert_one(collection: AsyncIOMotorCollection, doc: dict) -> ObjectId:
    doc.setdefault("created_at", now())
    doc.setdefault("updated_at", now())
    result = await collection.insert_one(doc)
    return result.inserted_id


async def find_by_id(collection: AsyncIOMotorCollection, oid: ObjectId) -> dict | None:
    return await collection.find_one({"_id": oid})


async def update_one(collection: AsyncIOMotorCollection, oid: ObjectId, update: dict) -> None:
    update.setdefault("$set", {})["updated_at"] = now()
    await collection.update_one({"_id": oid}, update)
