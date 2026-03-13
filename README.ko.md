<p align="center">
  <a href="./README.md"><img src="https://img.shields.io/badge/Language-EN-6B7280?style=for-the-badge" alt="English"></a>
  <a href="./README.ko.md"><img src="https://img.shields.io/badge/Language-KO-111111?style=for-the-badge" alt="한국어"></a>
</p>
<p align="center"><sub>Switch language / 언어 전환</sub></p>

# Agent Memory System | 에이전트 메모리 시스템

지속형 AI 비서를 위한 장기 기억 아키텍처입니다.

상태: Beta  
런타임: Python 3.11+  
저장소: MongoDB (Source of Truth) + ChromaDB (Vector Cache)

## 1. Introduction

Agent Memory System은 지속형 AI 비서를 위해 설계된 장기 기억 아키텍처입니다.

AI 모델은 보통 세션이 바뀌면 사실상 상태를 잃고 다시 시작합니다. 하지만 개인 비서는 며칠, 몇 주, 몇 달에 걸친 연속성이 필요합니다. 이 프로젝트는 그 연속성을 위한 전용 기억 계층을 제공합니다. 원시 대화 로그를 장기 기억의 기본 단위로 삼는 대신, 정제된 기억을 기본 단위로 두고 구조화된 회상, digest 계층, 토픽 계층을 통해 다시 활용할 수 있게 만듭니다.

시스템은 Model Context Protocol(MCP)로 노출되므로, 기본 기억 모델을 바꾸지 않고도 에이전트 런타임, CLI 도구, 개발 환경에 연결할 수 있습니다.

## 2. Design Philosophy

이 프로젝트는 모든 대화 턴을 영구 보관하는 방식으로 설계되지 않았습니다.

- memory는 로그가 아닙니다.
- memory는 선택적입니다.
- memory는 압축된 의미입니다.
- memory는 시간에 따라 다시 해석됩니다.
- `memory != conversation log`
- `memory = compacted meaning`

핵심 흐름은 다음과 같습니다.

```text
conversation
  -> candidate extraction / compaction
  -> memory
```

이 프로젝트는 모든 상호작용을 저장하려 하지 않습니다.

대신, 의미 있는 경험을 시간축 위에서 다시 사용할 수 있도록 정제한 표현으로 memory를 모델링합니다.

실제로는 다음을 뜻합니다.

- 원시 대화는 장기 기억의 정본 단위로 취급하지 않습니다.
- 기억의 품질은 클라이언트가 무엇을 저장하고 어떻게 요약하느냐에 달려 있습니다.
- 시스템은 시간 필터, 최신순 브라우징, digest 갱신 흐름을 지원합니다.
- 오래된 기억도 버려지지 않고 배경 맥락으로 남아 있습니다.
- digest 계층은 텍스트 누적이 아니라 해석된 의미를 이어가기 위해 존재합니다.

이 프로젝트는 vector database wrapper가 아닙니다.

지속형 AI 비서를 위한 memory architecture입니다.

## 3. Memory Model

### Memory Layers

이 시스템은 원시 대화 기록이 아니라 계층형 구조로 기억을 다룹니다.

```text
raw conversation (not the primary long-term unit)
  -> memory (structured unit)
  -> digest layers
  -> topic hierarchy
```

- `memory`는 사실, 선호, 계획, 사건처럼 의미 있는 내용을 담는 기본 저장 단위입니다.
- `digests`는 여러 memory를 일간, 주간, 월간, 연간 요약으로 압축합니다.
- `topic hierarchy`는 `topic_path(T1→T4)`를 통해 기억의 의미 계보를 정리합니다.

### Temporal Weighting

기억 회상은 시간 정보를 함께 고려합니다.

- `time_range`는 생성 시각 기준으로 후보 범위를 좁힙니다.
- query 없이 브라우징할 때는 최신 기억이 먼저 반환됩니다.
- 오래된 기억도 의미 검색, digest, topic context를 통해 계속 참조될 수 있습니다.
- 현재 ranked recall은 별도의 최신성 보너스가 아니라 memory 점수와 semantic similarity를 함께 사용합니다.

### Compaction Model (Client-Driven)

Compaction은 의도적으로 클라이언트 주도 방식입니다.

- 의미 해석은 실제 대화를 이해한 클라이언트 또는 비서가 담당합니다.
- 서버는 저장, 후보 선택, 수명주기 조정에 책임을 둡니다.
- 서버 내부에서 숨겨진 LLM 요약을 수행하지 않습니다.

운영 원칙은 단순합니다.

`memory quality is determined at the save stage`

Compaction 수명주기:

```text
experience / conversation
  -> candidate extraction / compaction
  -> memory
  -> digest
  -> hierarchy
```

실행 흐름:

1. `memory_save`가 compaction hint를 반환합니다.
2. 클라이언트가 `memory_compact(level=...)`를 호출합니다.
3. 서버가 원본 memory 또는 digest 후보를 반환합니다.
4. 클라이언트가 digest 텍스트를 작성합니다.
5. 클라이언트가 `memory_save(category="digest", ...)`로 저장합니다.

임계치:

- `L1`: yesterday uncompacted >= 3
- `L2`: last week L1 digests >= 5
- `L3`: last month L2 digests >= 3
- `L4`: last year L3 digests >= 6

## 4. Architecture

아키텍처는 MongoDB를 source of truth로 유지하고, ChromaDB를 재구축 가능한 벡터 캐시로 취급합니다.

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

핵심 아키텍처 결정:

- MongoDB는 지속 저장을 담당하는 단일 source of truth입니다.
- ChromaDB는 의미 검색을 위한 파생 캐시이며 MongoDB 기준으로 재구축할 수 있습니다.
- MCP 서버는 데이터 계층입니다. 내부에서 LLM을 호출하지 않습니다.
- 민감도 정책은 recall과 summarize 경계에서 적용됩니다.

## 5. MCP Interface

시스템 기능은 MCP를 통해 노출되므로, 여러 클라이언트가 같은 기억 모델을 공유할 수 있습니다.

MCP는 이 프로젝트의 정체성이라기보다 구현 인터페이스입니다.

### Tools

| Tool | 설명 |
|:--|:--|
| `memory_save` | 카테고리/중요도/민감도 기반 기억 저장 |
| `memory_recall` | 의미/날짜 기반 회상, similarity 재랭킹, debug/sensitive 옵션 지원 |
| `memory_summarize` | 카테고리/토픽별 기억 요약 |
| `session_digest` | 대화 내용을 기억 후보로 추출 |
| `memory_approve` | pending 기억 조회/승인/기각 |
| `memory_compact` | digest 생성을 위한 compaction 후보 반환 |
| `memory_update` | 기존 기억 필드 업데이트 |
| `memory_delete` | 기억 삭제 및 topic 카운트 동기화 |
| `topic_lookup` | 계층 토픽 검색/상세 조회 |
| `memory_policy` | 민감도 정책 조회/수정 |

### Resources

| Resource | 설명 |
|:--|:--|
| `user://profile` | 사용자 프로필 요약 |
| `memory://recent` | 최근 7일 기억 타임라인 |
| `memory://stats` | 기억 통계 및 카테고리 분포 |
| `memory://compaction-status` | 레벨별 compaction 준비 상태 |

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

`.env.example`는 host-side 실행 기준(`localhost:27018` / `localhost:8001`)으로 맞춰져 있습니다.
반면 app 컨테이너는 `docker-compose.yml`의 `DOCKER_*` override를 사용하므로, host 값이 컨테이너 내부 네트워크 설정에 섞이지 않습니다.

종료:

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

먼저 백엔드를 실행합니다.

```bash
docker compose up -d mongodb chroma
```

`PRELOAD_EMBEDDING_MODEL=false`를 Docker MCP 등록의 안전한 기본값으로 둡니다.
서버 시작 전에 임베딩 모델 다운로드가 걸리면 stdio가 열리기 전에 일부 클라이언트 등록이 timeout 날 수 있습니다.
처음에는 `false`로 두고 모델 다운로드/캐시가 끝난 뒤에만 `true`로 바꿔 startup preload를 쓰는 편이 안전합니다.

#### Option A. Local venv server

`<PROJECT_DIR>`를 로컬 저장소 경로로 바꿔 사용하세요.

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

`<PROJECT_DIR>`를 로컬 저장소 경로로 바꿔 사용하세요.

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

이 런처는 저장소 루트를 스스로 찾은 뒤 `docker compose ... app python -m src.server`를 실행합니다.
기본값은 `.env`의 `PRELOAD_EMBEDDING_MODEL=false`로 두고, 정말 필요할 때만 override해서 쓸 수 있습니다.

```bash
PRELOAD_EMBEDDING_MODEL=true ./scripts/run_mcp_docker.sh
```

### Usage Examples

기억 저장:

```json
{
  "name": "memory_save",
  "arguments": {
    "content": "사용자는 무설탕 탄산수를 선호한다.",
    "category": "preference",
    "importance": 8,
    "sensitivity": "normal"
  }
}
```

의미 + 날짜 회상:

```json
{
  "name": "memory_recall",
  "arguments": {
    "query": "탄산수 선호",
    "time_range": "30d",
    "top_k": 5
  }
}
```

쿼리 없이 날짜 브라우징:

```json
{
  "name": "memory_recall",
  "arguments": {
    "time_range": "7d",
    "include_debug": true
  }
}
```

민감정보 선택 확장:

```json
{
  "name": "memory_recall",
  "arguments": {
    "query": "개인 건강 정보",
    "include_sensitive": true
  }
}
```

### Sensitivity Policy Model

- 저장/수정 시 에이전트가 `sensitivity(normal|medium|high)`를 직접 지정합니다.
- 서버는 키워드 기반 자동 상향 판단을 하지 않습니다.
- `hide_sensitive_on_recall=true`이면 `high` 항목은 기본적으로 가려집니다.
- `include_sensitive=true`일 때만 상세 노출이 확장됩니다.

### Configuration

host-side `.env`는 compose가 열어둔 포트(`localhost:27018` / `localhost:8001`)를 바라보도록 둘 수 있고, `app` 서비스는 컨테이너 내부 연결값만 `DOCKER_*`로 별도 주입합니다.
`EMBEDDING_MODEL_NAME`를 공개 Hugging Face / `sentence-transformers` 모델 ID로 바꾸면 첫 사용 시 다운로드되고, Docker app은 이를 `DOCKER_EMBEDDING_CACHE_DIR` 아래에 캐시합니다.

| Env | 예시값 | 설명 |
|:--|:--|:--|
| `MONGODB_URI` | `mongodb://localhost:27018` | host-side Mongo 연결 URI |
| `DB_NAME` | `agent_memory` | 데이터베이스 이름 |
| `CHROMA_ENABLED` | `true` | Chroma 벡터 경로 활성화 |
| `CHROMA_HOST` | `localhost` | host-side Chroma 호스트 |
| `CHROMA_PORT` | `8001` | host-side Chroma 포트 |
| `CHROMA_SSL` | `false` | Chroma TLS 사용 여부 |
| `CHROMA_COLLECTION_NAME` | `memory_bge_m3_v1` | 활성 컬렉션 이름 |
| `EMBEDDING_MODEL_NAME` | `dragonkue/BGE-m3-ko` | 임베딩 모델 이름 |
| `EMBEDDING_DEVICE` | `cpu` | 임베딩 실행 디바이스 |
| `EMBEDDING_CACHE_DIR` | empty | 로컬 캐시 경로 |
| `EMBEDDING_MAX_CHARS` | `1200` | 임베딩 전 최대 문자 수 |
| `PRELOAD_EMBEDDING_MODEL` | `false` | MCP stdio 시작 전에 임베딩 모델을 preload할지 여부. 첫 Docker 등록은 `false`로 두고, 모델 캐시가 준비된 뒤에만 `true` 사용 권장 |
| `DOCKER_MONGODB_URI` | `mongodb://mongodb:27017` | app 컨테이너에서만 쓰는 Mongo URI |
| `DOCKER_CHROMA_HOST` | `chroma` | app 컨테이너에서만 쓰는 Chroma 호스트 |
| `DOCKER_CHROMA_PORT` | `8000` | app 컨테이너에서만 쓰는 Chroma 포트 |
| `DOCKER_EMBEDDING_CACHE_DIR` | `/app/.cache/models` | app 컨테이너에서만 쓰는 모델 캐시 경로 |
| `CENTROID_STALE_DAYS` | `14` | stale centroid 판단 일수 |
| `SIMILARITY_WEIGHT` | `15.0` | recall 점수 계산 시 의미 유사도 보너스 가중치 |

## 7. Benchmark

현재 스냅샷은 개발 당시 로컬 테스트를 기준으로 합니다.

실행 환경:

- `seed_count`: `30`
- `warmup`: `3`
- `chroma_enabled`: `True`
- `chroma_collection_name`: `memory_bge_m3_v1`
- `embedding_model_name`: `dragonkue/BGE-m3-ko`

지연시간 (ms):

| 지표 | Save | Recall |
|:--|--:|--:|
| count | 20 | 20 |
| avg | 6.97 | 342.70 |
| p50 | 6.56 | 308.06 |
| p95 | 8.54 | 505.40 |
| max | 8.75 | 692.27 |

메모리 사용량 (MB):

| 지표 | 값 |
|:--|--:|
| rss_max_mb_before | 74.59 |
| rss_max_mb_after | 2448.53 |
| rss_max_mb_increase | 2373.94 |

이 수치를 읽는 방법:

- recall은 벡터 조회, Mongo 후보 로딩, 점수 재정렬을 함께 수행하므로 save보다 무겁습니다.
- 큰 RSS 증가폭은 벤치마크 실행 중 프로세스 내부에 임베딩 모델이 상주하게 된 영향일 가능성이 큽니다.
- MongoDB가 source of truth이므로 ChromaDB는 `python scripts/rebuild_chroma.py`로 다시 만들 수 있는 캐시입니다.
- 이 분리는 저장 정합성을 단순하게 유지하면서도 의미 기반 recall과 캐시 재구축 워크플로우를 가능하게 합니다.

## 8. Project Scope

### Beta Scope

- 안정적인 MCP tool/resource 계약
- MongoDB source of truth + 재구축 가능한 Chroma cache
- 의미 기반 retrieval + 시간 필터 + 최신순 브라우징을 포함한 dual-layer recall
- 클라이언트 주도 compaction 워크플로우
- 기본 보호형 민감 기억 제어와 명시적 확장
- `topic_path(T1→T4)` 기반 topic hierarchy

### Project Layout

```text
src/
  server.py                MCP 서버 엔트리포인트
  tools/                   MCP 도구
  resources/               MCP 리소스
  db/                      Mongo 연결, CRUD, 인덱스
  engine/                  memory, recall, compaction, scoring 엔진
scripts/
  init_db.py               DB 초기화
  seed_rules.py            초기 rule seed
  init_profile.py          기본 profile 초기화
  rebuild_chroma.py        Mongo SoT 기준 벡터 캐시 재구축
docker-compose.yml         로컬 스택 (mongodb/chroma/app)
```

### Third-Party License Notice

| 컴포넌트 | 라이선스 | 참고 링크 |
|:--|:--|:--|
| 임베딩 모델 `dragonkue/BGE-m3-ko` | Apache License 2.0 | https://huggingface.co/dragonkue/BGE-m3-ko |
| ChromaDB (`chroma-core/chroma`) | Apache License 2.0 | https://github.com/chroma-core/chroma |
| MongoDB Community Server (`mongo:7`) | Server Side Public License (SSPL) v1 | https://github.com/mongodb/mongo |

각 컴포넌트의 사용 및 재배포는 해당 라이선스 조건을 따라야 합니다.
