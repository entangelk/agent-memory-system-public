import os
from dotenv import load_dotenv

load_dotenv()

MONGODB_URI: str = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
DB_NAME: str = os.getenv("DB_NAME", "agent_memory")


def _to_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _normalize_transport(value: str | None) -> str:
    if value is None:
        return "stdio"

    normalized = value.strip().lower().replace("_", "-")
    if normalized == "http":
        normalized = "streamable-http"

    if normalized not in {"stdio", "streamable-http"}:
        raise ValueError("MCP_TRANSPORT must be 'stdio' or 'streamable-http'")

    return normalized


def _normalize_http_path(value: str | None) -> str:
    if value is None:
        return "/mcp"

    normalized = value.strip()
    if not normalized:
        return "/mcp"
    if not normalized.startswith("/"):
        normalized = f"/{normalized}"

    return normalized

# Chroma integration (dual-layer centroid search)
CHROMA_ENABLED: bool = _to_bool(os.getenv("CHROMA_ENABLED"), default=False)
CHROMA_HOST: str = os.getenv("CHROMA_HOST", "chroma")
CHROMA_PORT: int = int(os.getenv("CHROMA_PORT", "8000"))
CHROMA_SSL: bool = _to_bool(os.getenv("CHROMA_SSL"), default=False)
CHROMA_COLLECTION_NAME: str = os.getenv("CHROMA_COLLECTION_NAME", "memory_bge_m3_v1")

# Embedding model (in-app)
EMBEDDING_MODEL_NAME: str = os.getenv("EMBEDDING_MODEL_NAME", "dragonkue/BGE-m3-ko")
EMBEDDING_DEVICE: str = os.getenv("EMBEDDING_DEVICE", "cpu")
EMBEDDING_CACHE_DIR: str = os.getenv("EMBEDDING_CACHE_DIR", "")
EMBEDDING_MAX_CHARS: int = int(os.getenv("EMBEDDING_MAX_CHARS", "1200"))
PRELOAD_EMBEDDING_MODEL: bool = _to_bool(os.getenv("PRELOAD_EMBEDDING_MODEL"), default=False)

# MCP transport
MCP_TRANSPORT: str = _normalize_transport(os.getenv("MCP_TRANSPORT"))
MCP_HTTP_HOST: str = os.getenv("MCP_HTTP_HOST", "127.0.0.1")
MCP_HTTP_PORT: int = int(os.getenv("MCP_HTTP_PORT", "8002"))
MCP_HTTP_PATH: str = _normalize_http_path(os.getenv("MCP_HTTP_PATH"))

# Centroid quality/staleness policy
CENTROID_STALE_DAYS: int = int(os.getenv("CENTROID_STALE_DAYS", "14"))

# Semantic similarity weight for combined scoring (reranking)
SIMILARITY_WEIGHT: float = float(os.getenv("SIMILARITY_WEIGHT", "15.0"))
