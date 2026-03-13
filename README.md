<p align="center">
  <a href="./README.md"><img src="https://img.shields.io/badge/Language-EN-111111?style=for-the-badge" alt="English"></a>
  <a href="./README.ko.md"><img src="https://img.shields.io/badge/Language-KO-6B7280?style=for-the-badge" alt="한국어"></a>
</p>
<p align="center"><sub>Switch language / 언어 전환</sub></p>

# Agent Memory System

Long-term memory architecture for persistent AI assistants.

Status: Beta  
Runtime: Python 3.11+  
Storage: MongoDB (Source of Truth) + ChromaDB (Vector Cache)

## 1. Introduction

Agent Memory System is a long-term memory architecture designed for persistent AI assistants.

AI models are typically stateless across sessions. A personal assistant, however, needs continuity across days, weeks, and months. This project provides that continuity as a dedicated memory layer: it treats distilled memories, not raw chat logs, as the primary long-term unit and keeps them available through structured recall, digest layers, and topic hierarchy.

The system is exposed through the Model Context Protocol (MCP) so it can integrate with agent runtimes, CLI tools, and development environments without changing the underlying memory model.

## 2. Design Philosophy

This project is not built around storing every conversation turn forever.

- Memory is not a log.
- Memory is selective.
- Memory is compacted meaning.
- Memory is reinterpreted over time.
- `memory != conversation log`
- `memory = compacted meaning`

The core flow is:

```text
conversation
  -> candidate extraction / compaction
  -> memory
```

This project does not attempt to store every interaction.

Instead, it models memory as a distilled, time-aware representation of meaningful experiences.

In practice, that means:

- Raw conversation is not treated as the canonical long-term unit.
- Memory quality depends on what the client decides to save and how it summarizes it.
- The system supports time-range filtering, recent-first browsing, and digest refresh over time.
- Older memories remain available as context instead of being discarded.
- Digest layers exist so the assistant can carry forward interpreted meaning, not just accumulated text.

This is not a vector database wrapper.

It is a memory architecture for persistent AI assistants.

## 3. Memory Model

### Memory Layers

The system organizes memory as a layered structure rather than raw transcript storage.

```text
raw conversation (not the primary long-term unit)
  -> memory (structured unit)
  -> digest layers
  -> topic hierarchy
```

- `memory` is the primary stored unit for meaningful facts, preferences, plans, and events.
- `digests` compact multiple memories into daily, weekly, monthly, and yearly summaries.
- `topic hierarchy` organizes memories by semantic lineage through `topic_path(T1→T4)`.

### Temporal Weighting

Memory retrieval is time-aware.

- `time_range` narrows candidate selection by creation time.
- Query-less browsing returns recent memories first.
- Older memories remain available through semantic retrieval, digests, and topic context.
- Ranked recall is currently driven by memory score plus semantic similarity, not by a separate recency bonus.

### Compaction Model (Client-Driven)

Compaction is intentionally client-driven.

- Meaning interpretation belongs to the client or assistant that actually understands the conversation.
- The server is responsible for storage, candidate selection, and lifecycle coordination.
- The server does not perform hidden LLM summarization internally.

The operating principle is simple:

`memory quality is determined at the save stage`

Compaction lifecycle:

```text
experience / conversation
  -> candidate extraction / compaction
  -> memory
  -> digest
  -> hierarchy
```

Operational flow:

1. `memory_save` returns a compaction hint.
2. The client calls `memory_compact(level=...)`.
3. The server returns raw source memories or digests.
4. The client writes the digest text.
5. The client saves the digest via `memory_save(category="digest", ...)`.

Thresholds:

- `L1`: yesterday uncompacted >= 3
- `L2`: last week L1 digests >= 5
- `L3`: last month L2 digests >= 3
- `L4`: last year L3 digests >= 6

## 4. Architecture

The architecture keeps MongoDB as the source of truth and treats ChromaDB as a rebuildable vector cache.

```mermaid
flowchart LR
    A[AI Client<br/>Claude Code / Cursor / etc.] -->|MCP stdio| B[Agent Memory Server]
    B --> C[Tool Layer]
    C --> D[(MongoDB<br/>Source of Truth)]
    C --> E[(ChromaDB<br/>Vector Cache)]
    C --> F[Compaction Hint<br/>Sensitivity Policy]
    D -. rebuild cache .-> E
```

### Recall Path (Dual-Layer)

```mermaid
sequenceDiagram
    participant Client as AI Client
    participant Server as MCP Server
    participant Chroma as ChromaDB
    participant Mongo as MongoDB
    Client->>Server: memory_recall(query, time_range, ...)
    Server->>Chroma: centroid search + memory-vector search
    Server->>Mongo: load candidates + fallback text search
    Server->>Server: combined scoring (memory score + similarity bonus)
    Server-->>Client: reranked results (+ redaction policy applied)
```

Key architectural decisions:

- MongoDB is the single source of truth for durable memory state.
- ChromaDB is derived cache for semantic retrieval and can be rebuilt from MongoDB.
- The MCP server is a data layer. It does not call an internal LLM.
- Sensitivity policy is enforced at recall and summary boundaries.

## 5. MCP Interface

The system exposes its functionality through MCP so that different clients can share the same memory model.

MCP is the interface layer, not the main identity of the project.

### Tools

| Tool | Description |
|:--|:--|
| `memory_save` | Save memory with category/importance/sensitivity |
| `memory_recall` | Semantic/date recall with similarity reranking, debug/sensitive options |
| `memory_summarize` | Summarize memories by category/topic |
| `session_digest` | Extract memory candidates from conversation |
| `memory_approve` | List/approve/dismiss pending memories |
| `memory_compact` | Return compaction candidates for digesting |
| `memory_update` | Update existing memory fields |
| `memory_delete` | Delete memory and sync topic counters |
| `topic_lookup` | Search and inspect topic hierarchy |
| `memory_policy` | Get/set sensitivity policy |

### Resources

| Resource | Description |
|:--|:--|
| `user://profile` | User profile summary |
| `memory://recent` | Recent memory timeline (7 days) |
| `memory://stats` | Memory statistics and category distribution |
| `memory://compaction-status` | Compaction readiness by level |

## 6. Usage

### Quick Start (Docker)

```bash
cp .env.example .env
docker compose build app
docker compose up -d mongodb chroma
docker compose run --rm app python scripts/init_db.py
docker compose run --rm app python scripts/seed_rules.py
docker compose run --rm app python scripts/init_profile.py
docker compose run --rm --no-deps -T app python -m src.server
```

`.env.example` is tuned for host-side runs (`localhost:27018` / `localhost:8001`).
The app container uses `DOCKER_*` overrides from `docker-compose.yml`, so host values do not leak into in-container networking.

Stop services:

```bash
docker compose down
```

### Local Run (Optional)

```bash
cp .env.example .env
docker compose up -d mongodb chroma
python3 -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
python scripts/init_db.py
python scripts/seed_rules.py
python scripts/init_profile.py
python -m src.server
```

### MCP Client Setup (Claude Code)

Run backend first:

```bash
docker compose up -d mongodb chroma
```

`PRELOAD_EMBEDDING_MODEL=false` is the safe default for Docker MCP registration.
If startup tries to download the embedding model before stdio is ready, some clients may time out during registration.
Keep it `false` for the first run, let the model download/cache complete, and only then switch it to `true` if you want startup preload.

#### Option A. Local venv server

Replace `<PROJECT_DIR>` with your local repository path.

```json
{
  "mcpServers": {
    "agent-memory": {
      "command": "/bin/bash",
      "args": [
        "-lc",
        "cd \"<PROJECT_DIR>\" && source .venv/bin/activate && python -m src.server"
      ]
    }
  }
}
```

#### Option B. Docker server

Replace `<PROJECT_DIR>` with your local repository path.

```json
{
  "mcpServers": {
    "agent-memory": {
      "command": "<PROJECT_DIR>/scripts/run_mcp_docker.sh",
      "args": []
    }
  }
}
```

The launcher resolves the repository root on its own and runs `docker compose ... app python -m src.server`.
You can keep `PRELOAD_EMBEDDING_MODEL=false` in `.env` or override it only when needed:

```bash
PRELOAD_EMBEDDING_MODEL=true ./scripts/run_mcp_docker.sh
```

### Usage Examples

Save memory:

```json
{
  "name": "memory_save",
  "arguments": {
    "content": "User prefers sugar-free sparkling water.",
    "category": "preference",
    "importance": 8,
    "sensitivity": "normal"
  }
}
```

Recall by meaning + date:

```json
{
  "name": "memory_recall",
  "arguments": {
    "query": "sparkling water preference",
    "time_range": "30d",
    "top_k": 5
  }
}
```

Date browsing without query:

```json
{
  "name": "memory_recall",
  "arguments": {
    "time_range": "7d",
    "include_debug": true
  }
}
```

Sensitive expansion on demand:

```json
{
  "name": "memory_recall",
  "arguments": {
    "query": "personal health",
    "include_sensitive": true
  }
}
```

### Sensitivity Policy Model

- The agent explicitly sets `sensitivity(normal|medium|high)` on save or update.
- The server does not auto-upgrade sensitivity by keyword guessing.
- With `hide_sensitive_on_recall=true`, `high` items are redacted by default.
- Details expand only when `include_sensitive=true` is explicitly set.

### Configuration

Host-side `.env` values can target the compose-published ports (`localhost:27018` / `localhost:8001`).
The `app` service rewrites container-only connection values with `DOCKER_*` overrides.
Changing `EMBEDDING_MODEL_NAME` to a public Hugging Face / `sentence-transformers` model ID downloads it on first use, and the Docker app caches it under `DOCKER_EMBEDDING_CACHE_DIR`.

| Env | Example | Description |
|:--|:--|:--|
| `MONGODB_URI` | `mongodb://localhost:27018` | Host-side Mongo connection URI |
| `DB_NAME` | `agent_memory` | Database name |
| `CHROMA_ENABLED` | `true` | Enable Chroma vector path |
| `CHROMA_HOST` | `localhost` | Host-side Chroma host |
| `CHROMA_PORT` | `8001` | Host-side Chroma port |
| `CHROMA_SSL` | `false` | Chroma TLS toggle |
| `CHROMA_COLLECTION_NAME` | `memory_bge_m3_v1` | Active collection name |
| `EMBEDDING_MODEL_NAME` | `dragonkue/BGE-m3-ko` | Embedding model name |
| `EMBEDDING_DEVICE` | `cpu` | Embedding runtime device |
| `EMBEDDING_CACHE_DIR` | empty | Local cache path |
| `EMBEDDING_MAX_CHARS` | `1200` | Max chars before embedding truncation |
| `PRELOAD_EMBEDDING_MODEL` | `false` | Preload the embedding model before MCP stdio starts; keep `false` for first Docker registration, switch to `true` only after the model cache is ready |
| `DOCKER_MONGODB_URI` | `mongodb://mongodb:27017` | Mongo URI used only by the app container |
| `DOCKER_CHROMA_HOST` | `chroma` | Chroma host used only by the app container |
| `DOCKER_CHROMA_PORT` | `8000` | Chroma port used only by the app container |
| `DOCKER_EMBEDDING_CACHE_DIR` | `/app/.cache/models` | Persistent model cache path used only by the app container |
| `CENTROID_STALE_DAYS` | `14` | Stale centroid threshold (days) |
| `SIMILARITY_WEIGHT` | `15.0` | Semantic similarity bonus weight for recall scoring |

## 7. Benchmark

Current snapshot from Local dev test.

Environment:

- `seed_count`: `30`
- `warmup`: `3`
- `chroma_enabled`: `True`
- `chroma_collection_name`: `memory_bge_m3_v1`
- `embedding_model_name`: `dragonkue/BGE-m3-ko`

Latency (ms):

| Metric | Save | Recall |
|:--|--:|--:|
| count | 20 | 20 |
| avg | 6.97 | 342.70 |
| p50 | 6.56 | 308.06 |
| p95 | 8.54 | 505.40 |
| max | 8.75 | 692.27 |

Memory Usage (MB):

| Metric | Value |
|:--|--:|
| rss_max_mb_before | 74.59 |
| rss_max_mb_after | 2448.53 |
| rss_max_mb_increase | 2373.94 |

How to read these numbers:

- Recall is heavier than save because it combines vector lookup, Mongo candidate loading, and score reranking.
- The large RSS increase likely reflects the in-process embedding model becoming resident during the benchmark run.
- MongoDB remains the source of truth, so ChromaDB can be rebuilt as cache with `python scripts/rebuild_chroma.py`.
- This separation keeps durable storage simple while allowing semantic recall and cache rebuild workflows.

## 8. Project Scope

### Beta Scope

- Stable MCP tool and resource contract
- MongoDB source of truth with rebuildable Chroma cache
- Dual-layer recall with semantic retrieval, time filters, and recent browsing
- Client-driven compaction workflow
- Safe-by-default sensitive-memory controls with explicit expansion
- Topic hierarchy through `topic_path(T1→T4)`

### Project Layout

```text
src/
  server.py                MCP server entry point
  tools/                   MCP tools
  resources/               MCP resources
  db/                      Mongo connection, CRUD, indexes
  engine/                  Memory, recall, compaction, scoring engines
scripts/
  init_db.py               DB bootstrap
  seed_rules.py            initial rule seed
  init_profile.py          default profile bootstrap
  rebuild_chroma.py        rebuild vector cache from Mongo SoT
docker-compose.yml         local stack (mongodb/chroma/app)
```

### Third-Party License Notice

| Component | License | Reference |
|:--|:--|:--|
| Embedding model `dragonkue/BGE-m3-ko` | Apache License 2.0 | https://huggingface.co/dragonkue/BGE-m3-ko |
| ChromaDB (`chroma-core/chroma`) | Apache License 2.0 | https://github.com/chroma-core/chroma |
| MongoDB Community Server (`mongo:7`) | Server Side Public License (SSPL) v1 | https://github.com/mongodb/mongo |

Use and redistribution of these components should follow each license's terms.
