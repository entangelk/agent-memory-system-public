from datetime import datetime, timedelta, UTC

from src.engine.scoring import retention_rate, calculate_score, calculate_combined_score


def test_retention_rate_handles_invalid_type():
    score = retention_rate("2026-02-24T12:00:00Z", recall_count=3)  # type: ignore[arg-type]
    assert score == 1.0


def test_retention_rate_handles_naive_datetime():
    naive_last_recalled = (datetime.now(UTC) - timedelta(days=2)).replace(tzinfo=None)
    score = retention_rate(naive_last_recalled, recall_count=3)
    assert isinstance(score, float)
    assert 0.0 < score <= 1.0


def test_retention_rate_handles_aware_datetime():
    aware_last_recalled = datetime.now(UTC) - timedelta(days=2)
    score = retention_rate(aware_last_recalled, recall_count=3)
    assert isinstance(score, float)
    assert 0.0 < score <= 1.0


def test_combined_score_adds_similarity_bonus(monkeypatch):
    from src import config
    monkeypatch.setattr(config, "SIMILARITY_WEIGHT", 15.0)

    memory = {"importance": 5, "recall_count": 0, "emotional_weight": 0.0}
    base = calculate_score(memory)

    combined_no_sim = calculate_combined_score(memory, similarity=0.0)
    assert combined_no_sim == base

    combined_full_sim = calculate_combined_score(memory, similarity=1.0)
    assert combined_full_sim == base + 15.0

    combined_half_sim = calculate_combined_score(memory, similarity=0.5)
    assert abs(combined_half_sim - (base + 7.5)) < 0.01


def test_combined_score_clamps_similarity(monkeypatch):
    from src import config
    monkeypatch.setattr(config, "SIMILARITY_WEIGHT", 10.0)

    memory = {"importance": 3}
    base = calculate_score(memory)

    assert calculate_combined_score(memory, similarity=-0.5) == base
    assert calculate_combined_score(memory, similarity=1.5) == base + 10.0
