"""
Chroma integration helper for dual-layer centroid retrieval.

Design goals:
- Optional and fail-safe: disabled/missing dependency/network error -> empty result.
- In-app embedding: sentence-transformers model loaded lazily.
"""
import asyncio
from threading import Lock
from typing import Any

from src import config

_MODEL: Any = None
_CLIENT: Any = None
_COLLECTION: Any = None
_EMBED_LOCK = Lock()


def chroma_enabled() -> bool:
    return config.CHROMA_ENABLED and bool(config.CHROMA_HOST)


def _get_model() -> Any | None:
    global _MODEL
    if _MODEL is not None:
        return _MODEL

    try:
        from sentence_transformers import SentenceTransformer
    except Exception:
        return None

    kwargs: dict[str, Any] = {"device": config.EMBEDDING_DEVICE}
    if config.EMBEDDING_CACHE_DIR:
        kwargs["cache_folder"] = config.EMBEDDING_CACHE_DIR

    try:
        _MODEL = SentenceTransformer(config.EMBEDDING_MODEL_NAME, **kwargs)
    except Exception:
        return None
    return _MODEL


def _get_collection() -> Any | None:
    global _CLIENT, _COLLECTION
    if _COLLECTION is not None:
        return _COLLECTION

    try:
        import chromadb
    except Exception:
        return None

    try:
        if _CLIENT is None:
            _CLIENT = chromadb.HttpClient(
                host=config.CHROMA_HOST,
                port=config.CHROMA_PORT,
                ssl=config.CHROMA_SSL,
            )
        _COLLECTION = _CLIENT.get_or_create_collection(
            name=config.CHROMA_COLLECTION_NAME,
            metadata={"hnsw:space": "cosine"},
        )
    except Exception:
        return None
    return _COLLECTION


def _embed_sync(text: str) -> list[float] | None:
    normalized = text.strip()
    if config.EMBEDDING_MAX_CHARS > 0:
        normalized = normalized[: config.EMBEDDING_MAX_CHARS]

    if not normalized:
        return None

    model = _get_model()
    if model is None:
        return None

    try:
        # Serialize encode calls to reduce memory spikes on low-spec machines.
        with _EMBED_LOCK:
            vec = model.encode(normalized, normalize_embeddings=True)
    except Exception:
        return None

    if hasattr(vec, "tolist"):
        return vec.tolist()
    return list(vec)


def _embed_many_sync(texts: list[str]) -> list[list[float]] | None:
    if not texts:
        return []

    normalized_list: list[str] = []
    for text in texts:
        normalized = text.strip() if isinstance(text, str) else ""
        if config.EMBEDDING_MAX_CHARS > 0:
            normalized = normalized[: config.EMBEDDING_MAX_CHARS]
        if not normalized:
            return None
        normalized_list.append(normalized)

    model = _get_model()
    if model is None:
        return None

    try:
        # Serialize encode calls to reduce memory spikes on low-spec machines.
        with _EMBED_LOCK:
            vecs = model.encode(normalized_list, normalize_embeddings=True)
    except Exception:
        return None

    if hasattr(vecs, "tolist"):
        converted = vecs.tolist()
    else:
        converted = [list(v) for v in vecs]

    if not converted:
        return None
    if isinstance(converted[0], (int, float)):
        converted = [converted]
    if len(converted) != len(normalized_list):
        return None
    return [list(v) for v in converted]


def _topic_id(topic_id: str) -> str:
    return f"topic:{topic_id}"


def _memory_id(memory_id: str) -> str:
    return f"memory:{memory_id}"


def _strip_prefix(value: str, prefix: str) -> str:
    if value.startswith(prefix):
        return value[len(prefix):]
    return value


def set_collection_name(name: str) -> None:
    """Switch the active collection, typically during reindex or migration."""
    global _COLLECTION
    if isinstance(name, str) and name.strip():
        config.CHROMA_COLLECTION_NAME = name.strip()
    _COLLECTION = None


def _upsert_topic_centroid_sync(
    *,
    topic_id: str,
    title: str,
    summary: str,
    memory_count: int = 0,
    metadata: dict[str, Any] | None = None,
) -> str | None:
    if not chroma_enabled():
        return None

    collection = _get_collection()
    if collection is None:
        return None

    doc = summary.strip() if summary else title.strip()
    if not doc:
        return None

    embedding = _embed_sync(doc)
    if embedding is None:
        return None

    vector_id = _topic_id(topic_id)
    meta = {
        "topic_id": topic_id,
        "type": "centroid",
        "title": title,
        "memory_count": int(memory_count),
        "embedding_model": config.EMBEDDING_MODEL_NAME,
    }
    if metadata:
        meta.update(metadata)

    try:
        collection.upsert(
            ids=[vector_id],
            embeddings=[embedding],
            documents=[doc],
            metadatas=[meta],
        )
    except Exception:
        return None
    return vector_id


async def upsert_topic_centroid(
    *,
    topic_id: str,
    title: str,
    summary: str,
    memory_count: int = 0,
    metadata: dict[str, Any] | None = None,
) -> str | None:
    return await asyncio.to_thread(
        _upsert_topic_centroid_sync,
        topic_id=topic_id,
        title=title,
        summary=summary,
        memory_count=memory_count,
        metadata=metadata,
    )


def _upsert_topic_centroids_batch_sync(items: list[dict[str, Any]]) -> tuple[int, int]:
    if not chroma_enabled():
        return 0, len(items)

    collection = _get_collection()
    if collection is None:
        return 0, len(items)

    prepared: list[dict[str, Any]] = []
    failed = 0
    for raw in items:
        if not isinstance(raw, dict):
            failed += 1
            continue

        topic_id = str(raw.get("topic_id", "")).strip()
        if not topic_id:
            failed += 1
            continue

        title = str(raw.get("title", "") or "").strip()
        summary = str(raw.get("summary", "") or "").strip()
        doc = summary if summary else title
        if config.EMBEDDING_MAX_CHARS > 0:
            doc = doc[: config.EMBEDDING_MAX_CHARS]
        if not doc:
            failed += 1
            continue

        try:
            memory_count = int(raw.get("memory_count", 0) or 0)
        except Exception:
            memory_count = 0

        meta = {
            "topic_id": topic_id,
            "type": "centroid",
            "title": title,
            "memory_count": memory_count,
            "embedding_model": config.EMBEDDING_MODEL_NAME,
        }
        metadata = raw.get("metadata")
        if isinstance(metadata, dict):
            meta.update(metadata)

        prepared.append({
            "id": _topic_id(topic_id),
            "doc": doc,
            "meta": meta,
        })

    if not prepared:
        return 0, failed

    embeddings = _embed_many_sync([row["doc"] for row in prepared])
    if embeddings is None:
        return 0, failed + len(prepared)

    try:
        collection.upsert(
            ids=[row["id"] for row in prepared],
            embeddings=embeddings,
            documents=[row["doc"] for row in prepared],
            metadatas=[row["meta"] for row in prepared],
        )
    except Exception:
        return 0, failed + len(prepared)
    return len(prepared), failed


async def upsert_topic_centroids_batch(items: list[dict[str, Any]]) -> tuple[int, int]:
    return await asyncio.to_thread(_upsert_topic_centroids_batch_sync, items)


def _upsert_memory_vector_sync(
    *,
    memory_id: str,
    content: str,
    category: str = "",
    importance: int = 0,
    topic_id: str = "",
    metadata: dict[str, Any] | None = None,
) -> str | None:
    if not chroma_enabled():
        return None

    collection = _get_collection()
    if collection is None:
        return None

    doc = content.strip() if isinstance(content, str) else ""
    if not doc:
        return None

    embedding = _embed_sync(doc)
    if embedding is None:
        return None

    vector_id = _memory_id(memory_id)
    meta = {
        "memory_id": memory_id,
        "type": "memory",
        "category": category,
        "importance": int(importance),
        "topic_id": topic_id,
        "embedding_model": config.EMBEDDING_MODEL_NAME,
    }
    if metadata:
        meta.update(metadata)

    try:
        collection.upsert(
            ids=[vector_id],
            embeddings=[embedding],
            documents=[doc],
            metadatas=[meta],
        )
    except Exception:
        return None
    return vector_id


async def upsert_memory_vector(
    *,
    memory_id: str,
    content: str,
    category: str = "",
    importance: int = 0,
    topic_id: str = "",
    metadata: dict[str, Any] | None = None,
) -> str | None:
    return await asyncio.to_thread(
        _upsert_memory_vector_sync,
        memory_id=memory_id,
        content=content,
        category=category,
        importance=importance,
        topic_id=topic_id,
        metadata=metadata,
    )


def _search_topic_ids_sync(
    *,
    query: str,
    top_k: int,
    filters: dict[str, Any] | None = None,
) -> list[str]:
    if not chroma_enabled():
        return []

    collection = _get_collection()
    if collection is None:
        return []

    embedding = _embed_sync(query)
    if embedding is None:
        return []

    try:
        where_filter: dict[str, Any] = {"type": "centroid"}
        if filters:
            where_filter.update(filters)
        kwargs: dict[str, Any] = {
            "query_embeddings": [embedding],
            "n_results": max(1, int(top_k)),
            "where": where_filter,
        }
        result = collection.query(**kwargs)
    except Exception:
        return []

    rows = result.get("ids") if isinstance(result, dict) else None
    if not rows or not isinstance(rows, list) or not rows[0]:
        return []

    topic_ids: list[str] = []
    for raw in rows[0]:
        if not isinstance(raw, str):
            continue
        if raw.startswith("topic:"):
            topic_ids.append(raw[6:])
        else:
            topic_ids.append(raw)
    return topic_ids[:top_k]


async def search_topic_ids(
    *,
    query: str,
    top_k: int,
    filters: dict[str, Any] | None = None,
) -> list[str]:
    return await asyncio.to_thread(
        _search_topic_ids_sync,
        query=query,
        top_k=top_k,
        filters=filters,
    )


def _search_topic_ids_with_scores_sync(
    *,
    query: str,
    top_k: int,
    filters: dict[str, Any] | None = None,
) -> list[tuple[str, float]]:
    """Search topic centroids and return cosine-similarity scores."""
    if not chroma_enabled():
        return []

    collection = _get_collection()
    if collection is None:
        return []

    embedding = _embed_sync(query)
    if embedding is None:
        return []

    try:
        where_filter: dict[str, Any] = {"type": "centroid"}
        if filters:
            where_filter.update(filters)
        result = collection.query(
            query_embeddings=[embedding],
            n_results=max(1, int(top_k)),
            where=where_filter,
            include=["distances"],
        )
    except Exception:
        return []

    rows = result.get("ids") if isinstance(result, dict) else None
    if not rows or not isinstance(rows, list) or not rows[0]:
        return []

    distances = result.get("distances", [[]])
    dist_list = distances[0] if distances and isinstance(distances[0], list) else []

    pairs: list[tuple[str, float]] = []
    for i, raw in enumerate(rows[0]):
        if not isinstance(raw, str):
            continue
        tid = raw[6:] if raw.startswith("topic:") else raw
        dist = dist_list[i] if i < len(dist_list) else 1.0
        similarity = max(0.0, 1.0 - float(dist))
        pairs.append((tid, similarity))
    return pairs[:top_k]


async def search_topic_ids_with_scores(
    *,
    query: str,
    top_k: int,
    filters: dict[str, Any] | None = None,
) -> list[tuple[str, float]]:
    return await asyncio.to_thread(
        _search_topic_ids_with_scores_sync,
        query=query,
        top_k=top_k,
        filters=filters,
    )


def _search_memory_ids_sync(
    *,
    query: str | None = None,
    top_k: int,
    filters: dict[str, Any] | None = None,
) -> list[str]:
    if not chroma_enabled():
        return []

    collection = _get_collection()
    if collection is None:
        return []

    limit = max(1, int(top_k))
    where_filter: dict[str, Any] = {"type": "memory"}
    if filters:
        where_filter.update(filters)

    raw_ids: list[str] = []
    if isinstance(query, str) and query.strip():
        embedding = _embed_sync(query)
        if embedding is None:
            return []
        try:
            result = collection.query(
                query_embeddings=[embedding],
                n_results=limit,
                where=where_filter,
            )
        except Exception:
            return []

        rows = result.get("ids") if isinstance(result, dict) else None
        if not rows or not isinstance(rows, list) or not rows[0]:
            return []
        first_row = rows[0]
        if not isinstance(first_row, list):
            return []
        raw_ids = [row for row in first_row if isinstance(row, str)]
    else:
        # Query-less mode is supported only for filter browsing such as date/category scans.
        if not filters:
            return []
        try:
            result = collection.get(where=where_filter, limit=limit)
        except Exception:
            return []
        rows = result.get("ids") if isinstance(result, dict) else None
        if not rows:
            return []
        if isinstance(rows, list) and rows and isinstance(rows[0], list):
            flattened = rows[0]
        elif isinstance(rows, list):
            flattened = rows
        else:
            return []
        raw_ids = [row for row in flattened if isinstance(row, str)]

    memory_ids = [_strip_prefix(raw, "memory:") for raw in raw_ids]
    return memory_ids[:limit]


async def search_memory_ids(
    *,
    query: str | None = None,
    top_k: int,
    filters: dict[str, Any] | None = None,
) -> list[str]:
    return await asyncio.to_thread(
        _search_memory_ids_sync,
        query=query,
        top_k=top_k,
        filters=filters,
    )


def _search_memory_ids_with_scores_sync(
    *,
    query: str | None = None,
    top_k: int,
    filters: dict[str, Any] | None = None,
) -> list[tuple[str, float]]:
    """Search memory vectors and return cosine scores; query-less browsing uses similarity=0."""
    if not chroma_enabled():
        return []

    collection = _get_collection()
    if collection is None:
        return []

    limit = max(1, int(top_k))
    where_filter: dict[str, Any] = {"type": "memory"}
    if filters:
        where_filter.update(filters)

    if isinstance(query, str) and query.strip():
        embedding = _embed_sync(query)
        if embedding is None:
            return []
        try:
            result = collection.query(
                query_embeddings=[embedding],
                n_results=limit,
                where=where_filter,
                include=["distances"],
            )
        except Exception:
            return []

        rows = result.get("ids") if isinstance(result, dict) else None
        if not rows or not isinstance(rows, list) or not rows[0]:
            return []
        first_row = rows[0]
        if not isinstance(first_row, list):
            return []

        distances = result.get("distances", [[]])
        dist_list = distances[0] if distances and isinstance(distances[0], list) else []

        pairs: list[tuple[str, float]] = []
        for i, raw in enumerate(first_row):
            if not isinstance(raw, str):
                continue
            mid = _strip_prefix(raw, "memory:")
            dist = dist_list[i] if i < len(dist_list) else 1.0
            similarity = max(0.0, 1.0 - float(dist))
            pairs.append((mid, similarity))
        return pairs[:limit]
    else:
        if not filters:
            return []
        try:
            result = collection.get(where=where_filter, limit=limit)
        except Exception:
            return []
        rows = result.get("ids") if isinstance(result, dict) else None
        if not rows:
            return []
        if isinstance(rows, list) and rows and isinstance(rows[0], list):
            flattened = rows[0]
        elif isinstance(rows, list):
            flattened = rows
        else:
            return []
        return [(_strip_prefix(raw, "memory:"), 0.0) for raw in flattened if isinstance(raw, str)][:limit]


async def search_memory_ids_with_scores(
    *,
    query: str | None = None,
    top_k: int,
    filters: dict[str, Any] | None = None,
) -> list[tuple[str, float]]:
    return await asyncio.to_thread(
        _search_memory_ids_with_scores_sync,
        query=query,
        top_k=top_k,
        filters=filters,
    )


def _delete_topic_centroid_sync(topic_id: str) -> None:
    if not chroma_enabled():
        return

    collection = _get_collection()
    if collection is None:
        return

    try:
        collection.delete(ids=[_topic_id(topic_id)])
    except Exception:
        return


async def delete_topic_centroid(topic_id: str) -> None:
    await asyncio.to_thread(_delete_topic_centroid_sync, topic_id)


def _clear_topic_centroids_sync() -> int:
    if not chroma_enabled():
        return 0

    collection = _get_collection()
    if collection is None:
        return 0

    try:
        result = collection.delete(where={"type": "centroid"})
    except Exception:
        return 0
    deleted = result.get("count") if isinstance(result, dict) else 0
    if not isinstance(deleted, int):
        return 0
    return deleted


async def clear_topic_centroids() -> int:
    return await asyncio.to_thread(_clear_topic_centroids_sync)


def _clear_memory_vectors_sync() -> int:
    if not chroma_enabled():
        return 0

    collection = _get_collection()
    if collection is None:
        return 0

    try:
        result = collection.delete(where={"type": "memory"})
    except Exception:
        return 0
    deleted = result.get("count") if isinstance(result, dict) else 0
    if not isinstance(deleted, int):
        return 0
    return deleted


async def clear_memory_vectors() -> int:
    return await asyncio.to_thread(_clear_memory_vectors_sync)
