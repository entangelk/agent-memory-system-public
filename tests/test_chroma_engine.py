from src import config
from src.engine import chroma_engine


def test_search_topic_ids_forces_centroid_where(monkeypatch):
    query_kwargs: dict = {}

    class DummyCollection:
        def query(self, **kwargs):
            query_kwargs.update(kwargs)
            return {"ids": [["topic:abc123"]]}

    monkeypatch.setattr(config, "CHROMA_ENABLED", True)
    monkeypatch.setattr(config, "CHROMA_HOST", "localhost")
    monkeypatch.setattr(chroma_engine, "_get_collection", lambda: DummyCollection())
    monkeypatch.setattr(chroma_engine, "_embed_sync", lambda text: [0.1, 0.2, 0.3])

    result = chroma_engine._search_topic_ids_sync(query="hello", top_k=3, filters=None)

    assert result == ["abc123"]
    assert query_kwargs.get("where") == {"type": "centroid"}


def test_search_topic_ids_merges_custom_where(monkeypatch):
    query_kwargs: dict = {}

    class DummyCollection:
        def query(self, **kwargs):
            query_kwargs.update(kwargs)
            return {"ids": [["topic:abc123"]]}

    monkeypatch.setattr(config, "CHROMA_ENABLED", True)
    monkeypatch.setattr(config, "CHROMA_HOST", "localhost")
    monkeypatch.setattr(chroma_engine, "_get_collection", lambda: DummyCollection())
    monkeypatch.setattr(chroma_engine, "_embed_sync", lambda text: [0.1, 0.2, 0.3])

    result = chroma_engine._search_topic_ids_sync(
        query="hello",
        top_k=3,
        filters={"status": "active"},
    )

    assert result == ["abc123"]
    assert query_kwargs.get("where") == {"type": "centroid", "status": "active"}


def test_upsert_topic_centroids_batch_sync_success(monkeypatch):
    upsert_kwargs: dict = {}

    class DummyCollection:
        def upsert(self, **kwargs):
            upsert_kwargs.update(kwargs)

    monkeypatch.setattr(config, "CHROMA_ENABLED", True)
    monkeypatch.setattr(config, "CHROMA_HOST", "localhost")
    monkeypatch.setattr(chroma_engine, "_get_collection", lambda: DummyCollection())
    monkeypatch.setattr(chroma_engine, "_embed_many_sync", lambda docs: [[0.1], [0.2]])

    indexed, failed = chroma_engine._upsert_topic_centroids_batch_sync([
        {
            "topic_id": "t1",
            "title": "Topic One",
            "summary": "",
            "memory_count": 3,
            "metadata": {"centroid_version": 2},
        },
        {
            "topic_id": "t2",
            "title": "Topic Two",
            "summary": "summary two",
            "memory_count": 5,
        },
    ])

    assert indexed == 2
    assert failed == 0
    assert upsert_kwargs.get("ids") == ["topic:t1", "topic:t2"]
    assert upsert_kwargs.get("documents") == ["Topic One", "summary two"]
    assert len(upsert_kwargs.get("metadatas", [])) == 2
    assert upsert_kwargs["metadatas"][0]["type"] == "centroid"
    assert upsert_kwargs["metadatas"][0]["centroid_version"] == 2


def test_upsert_topic_centroids_batch_sync_counts_invalid_rows(monkeypatch):
    class DummyCollection:
        def upsert(self, **kwargs):
            return None

    monkeypatch.setattr(config, "CHROMA_ENABLED", True)
    monkeypatch.setattr(config, "CHROMA_HOST", "localhost")
    monkeypatch.setattr(chroma_engine, "_get_collection", lambda: DummyCollection())
    monkeypatch.setattr(chroma_engine, "_embed_many_sync", lambda docs: [[0.1]])

    indexed, failed = chroma_engine._upsert_topic_centroids_batch_sync([
        {"topic_id": "ok", "title": "valid"},
        {"topic_id": "", "title": "invalid"},
        {"topic_id": "no_doc", "title": "", "summary": ""},
    ])

    assert indexed == 1
    assert failed == 2


def test_search_memory_ids_uses_memory_where(monkeypatch):
    query_kwargs: dict = {}

    class DummyCollection:
        def query(self, **kwargs):
            query_kwargs.update(kwargs)
            return {"ids": [["memory:m1", "memory:m2"]]}

    monkeypatch.setattr(config, "CHROMA_ENABLED", True)
    monkeypatch.setattr(config, "CHROMA_HOST", "localhost")
    monkeypatch.setattr(chroma_engine, "_get_collection", lambda: DummyCollection())
    monkeypatch.setattr(chroma_engine, "_embed_sync", lambda text: [0.1, 0.2, 0.3])

    result = chroma_engine._search_memory_ids_sync(query="hello", top_k=2, filters=None)

    assert result == ["m1", "m2"]
    assert query_kwargs.get("where") == {"type": "memory"}


def test_search_memory_ids_merges_custom_filters(monkeypatch):
    query_kwargs: dict = {}

    class DummyCollection:
        def query(self, **kwargs):
            query_kwargs.update(kwargs)
            return {"ids": [["memory:m1"]]}

    monkeypatch.setattr(config, "CHROMA_ENABLED", True)
    monkeypatch.setattr(config, "CHROMA_HOST", "localhost")
    monkeypatch.setattr(chroma_engine, "_get_collection", lambda: DummyCollection())
    monkeypatch.setattr(chroma_engine, "_embed_sync", lambda text: [0.1, 0.2, 0.3])

    result = chroma_engine._search_memory_ids_sync(
        query="hello",
        top_k=3,
        filters={"created_at_ts": {"$gte": 1000}},
    )

    assert result == ["m1"]
    assert query_kwargs.get("where") == {"type": "memory", "created_at_ts": {"$gte": 1000}}


def test_search_memory_ids_supports_filter_only_get(monkeypatch):
    get_kwargs: dict = {}

    class DummyCollection:
        def get(self, **kwargs):
            get_kwargs.update(kwargs)
            return {"ids": ["memory:only1"]}

    monkeypatch.setattr(config, "CHROMA_ENABLED", True)
    monkeypatch.setattr(config, "CHROMA_HOST", "localhost")
    monkeypatch.setattr(chroma_engine, "_get_collection", lambda: DummyCollection())

    result = chroma_engine._search_memory_ids_sync(
        query=None,
        top_k=5,
        filters={"created_at_ts": {"$gte": 2000}},
    )

    assert result == ["only1"]
    assert get_kwargs.get("where") == {"type": "memory", "created_at_ts": {"$gte": 2000}}


def test_search_memory_ids_filter_only_requires_filters(monkeypatch):
    class DummyCollection:
        def get(self, **kwargs):
            raise AssertionError("collection.get should not be called without filters")

    monkeypatch.setattr(config, "CHROMA_ENABLED", True)
    monkeypatch.setattr(config, "CHROMA_HOST", "localhost")
    monkeypatch.setattr(chroma_engine, "_get_collection", lambda: DummyCollection())

    result = chroma_engine._search_memory_ids_sync(query=None, top_k=3, filters=None)
    assert result == []


def test_search_topic_ids_with_scores_returns_similarity(monkeypatch):
    class DummyCollection:
        def query(self, **kwargs):
            return {
                "ids": [["topic:t1", "topic:t2"]],
                "distances": [[0.2, 0.6]],
            }

    monkeypatch.setattr(config, "CHROMA_ENABLED", True)
    monkeypatch.setattr(config, "CHROMA_HOST", "localhost")
    monkeypatch.setattr(chroma_engine, "_get_collection", lambda: DummyCollection())
    monkeypatch.setattr(chroma_engine, "_embed_sync", lambda text: [0.1])

    result = chroma_engine._search_topic_ids_with_scores_sync(query="hello", top_k=5)

    assert len(result) == 2
    assert result[0] == ("t1", 0.8)
    assert result[1] == ("t2", 0.4)


def test_search_memory_ids_with_scores_returns_similarity(monkeypatch):
    class DummyCollection:
        def query(self, **kwargs):
            return {
                "ids": [["memory:m1", "memory:m2"]],
                "distances": [[0.1, 0.5]],
            }

    monkeypatch.setattr(config, "CHROMA_ENABLED", True)
    monkeypatch.setattr(config, "CHROMA_HOST", "localhost")
    monkeypatch.setattr(chroma_engine, "_get_collection", lambda: DummyCollection())
    monkeypatch.setattr(chroma_engine, "_embed_sync", lambda text: [0.1])

    result = chroma_engine._search_memory_ids_with_scores_sync(query="hello", top_k=5)

    assert len(result) == 2
    assert result[0] == ("m1", 0.9)
    assert result[1] == ("m2", 0.5)


def test_search_memory_ids_with_scores_filter_only_returns_zero_similarity(monkeypatch):
    class DummyCollection:
        def get(self, **kwargs):
            return {"ids": ["memory:m1", "memory:m2"]}

    monkeypatch.setattr(config, "CHROMA_ENABLED", True)
    monkeypatch.setattr(config, "CHROMA_HOST", "localhost")
    monkeypatch.setattr(chroma_engine, "_get_collection", lambda: DummyCollection())

    result = chroma_engine._search_memory_ids_with_scores_sync(
        query=None, top_k=5, filters={"created_at_ts": {"$gte": 1000}},
    )

    assert len(result) == 2
    assert result[0] == ("m1", 0.0)
    assert result[1] == ("m2", 0.0)
