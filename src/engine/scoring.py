import math
from datetime import datetime, UTC

from src import config


def retention_rate(last_recalled: datetime | None, recall_count: int) -> float:
    """Approximate an Ebbinghaus-style forgetting curve that stabilizes with recall."""
    if last_recalled is None:
        return 1.0
    if not isinstance(last_recalled, datetime):
        return 1.0
    if last_recalled.tzinfo is None:
        # Mongo may deserialize datetimes as naive values when tz_aware=False.
        last_recalled = last_recalled.replace(tzinfo=UTC)
    days = (datetime.now(UTC) - last_recalled).days
    stability = recall_count * 1.5
    return math.exp(-days / max(stability, 1))


def calculate_score(memory: dict) -> float:
    """Compute the memory score from importance, reuse, emotion, and forgetting."""
    score = 0.0
    score += memory.get("importance", 0) * 3
    score += memory.get("recall_count", 0) * 2
    score += memory.get("emotional_weight", 0.0) * 1.5
    rate = retention_rate(memory.get("last_recalled"), memory.get("recall_count", 0))
    return score * rate


def calculate_combined_score(memory: dict, similarity: float = 0.0) -> float:
    """Add a bounded semantic-similarity bonus to the base memory score."""
    base = calculate_score(memory)
    return base + max(0.0, min(1.0, similarity)) * config.SIMILARITY_WEIGHT
