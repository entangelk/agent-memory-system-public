"""
memory_recall 골든 회귀 테스트.
고정 질문셋(20개)에서 기대 메모리 ID/키워드가 유지되는지 검증한다.
"""
import json
import pytest
from src.db.connection import get_db
from src.engine.memory_engine import save_memory
from src.tools.memory_recall import handle

TEST_CONTEXT = "recall_golden_regression_ctx"

GOLDEN_MEMORIES = {
    "m01": {
        "content": "GOLDEN_01_CARD_CENTER 나는 1시간 뒤에 영업센터에서 카드를 받는다.",
        "category": "event",
        "importance": 8,
    },
    "m02": {
        "content": "GOLDEN_02_CARD_COPY 카드 수령 전에 신분증 사본을 출력한다.",
        "category": "event",
        "importance": 7,
    },
    "m03": {
        "content": "GOLDEN_03_DEV_PYTEST recall 회귀 검증은 include_debug JSON을 사용한다.",
        "category": "fact",
        "importance": 6,
    },
    "m04": {
        "content": "GOLDEN_04_DEV_CHROMA Chroma 재색인은 scripts/reindex_chroma.py 경로를 사용한다.",
        "category": "fact",
        "importance": 6,
    },
    "m05": {
        "content": "GOLDEN_05_PRIVACY 민감 회상은 기본적으로 요약만 먼저 보여준다.",
        "category": "preference",
        "importance": 7,
    },
    "m06": {
        "content": "GOLDEN_06_BENCH_N150 벤치마크 기준 장비는 N150 8GB RAM이다.",
        "category": "fact",
        "importance": 6,
    },
    "m07": {
        "content": "GOLDEN_07_TOPIC_LAYER digest는 daily weekly monthly yearly 토픽 레이어에 연결된다.",
        "category": "fact",
        "importance": 6,
    },
    "m08": {
        "content": "GOLDEN_08_RESIDUAL_JSON residual_info는 유동 JSON 슬롯으로 저장한다.",
        "category": "fact",
        "importance": 6,
    },
    "m09": {
        "content": "GOLDEN_09_COMPACTION_THRESHOLD L1 임계치 3건, L2 임계치 5건을 사용한다.",
        "category": "fact",
        "importance": 7,
    },
    "m10": {
        "content": "GOLDEN_10_MCP_CLAUDE Claude Code MCP 등록은 ~/.claude.json mcpServers 설정으로 한다.",
        "category": "fact",
        "importance": 6,
    },
    "m11": {
        "content": "GOLDEN_11_GIT_PUSH 배포 전에는 테스트 통과 후 git push를 수행한다.",
        "category": "event",
        "importance": 5,
    },
    "m12": {
        "content": "GOLDEN_12_DOCKER_UP 로컬 실행 순서는 docker compose up -d mongodb chroma 이다.",
        "category": "event",
        "importance": 5,
    },
    "m13": {
        "content": "GOLDEN_13_EMBED_MODEL 임베딩 모델은 dragonkue/BGE-m3-ko 를 사용한다.",
        "category": "fact",
        "importance": 8,
    },
    "m14": {
        "content": "GOLDEN_14_LOCK_ENCODE encode lock은 저사양 메모리 스파이크를 줄인다.",
        "category": "fact",
        "importance": 6,
    },
    "m15": {
        "content": "GOLDEN_15_STALE_POLICY centroid stale 기본 정책값은 14일이다.",
        "category": "fact",
        "importance": 7,
    },
    "m16": {
        "content": "GOLDEN_16_REINDEX_CLEAR reindex --clear 옵션은 기존 centroid 벡터를 비우고 다시 적재한다.",
        "category": "fact",
        "importance": 6,
    },
    "m17": {
        "content": "GOLDEN_17_TOPIC_LOOKUP topic_lookup 도구는 토픽 계층 검색과 상세 조회를 제공한다.",
        "category": "fact",
        "importance": 6,
    },
    "m18": {
        "content": "GOLDEN_18_MEMORY_DELETE memory_delete 는 topics.memory_count 동기화를 수행한다.",
        "category": "fact",
        "importance": 6,
    },
    "m19": {
        "content": "GOLDEN_19_SENSITIVITY 민감 메모는 기본 숨김 후 요청 시 펼치는 UX가 필요하다.",
        "category": "emotion",
        "importance": 7,
    },
    "m20": {
        "content": "GOLDEN_20_WORK_LOG AGENTS 규칙에 따라 work_log와 HANDOFF를 항상 갱신한다.",
        "category": "event",
        "importance": 5,
    },
}

GOLDEN_CASES = [
    {"id": "c01", "query": "GOLDEN_01_CARD_CENTER", "expect_key": "m01", "keywords": ["영업센터", "카드"]},
    {"id": "c02", "query": "GOLDEN_02_CARD_COPY", "expect_key": "m02", "keywords": ["신분증", "사본"]},
    {"id": "c03", "query": "GOLDEN_03_DEV_PYTEST", "expect_key": "m03", "keywords": ["include_debug", "JSON"]},
    {"id": "c04", "query": "GOLDEN_04_DEV_CHROMA", "expect_key": "m04", "keywords": ["reindex_chroma.py", "Chroma"]},
    {"id": "c05", "query": "GOLDEN_05_PRIVACY", "expect_key": "m05", "keywords": ["민감", "요약"], "category": "preference"},
    {"id": "c06", "query": "GOLDEN_06_BENCH_N150", "expect_key": "m06", "keywords": ["N150", "8GB"]},
    {"id": "c07", "query": "GOLDEN_07_TOPIC_LAYER", "expect_key": "m07", "keywords": ["daily", "yearly"]},
    {"id": "c08", "query": "GOLDEN_08_RESIDUAL_JSON", "expect_key": "m08", "keywords": ["residual_info", "JSON"]},
    {"id": "c09", "query": "GOLDEN_09_COMPACTION_THRESHOLD", "expect_key": "m09", "keywords": ["L1", "L2"]},
    {"id": "c10", "query": "GOLDEN_10_MCP_CLAUDE", "expect_key": "m10", "keywords": ["mcpServers", "Claude"]},
    {"id": "c11", "query": "GOLDEN_11_GIT_PUSH", "expect_key": "m11", "keywords": ["git push"], "category": "event"},
    {"id": "c12", "query": "GOLDEN_12_DOCKER_UP", "expect_key": "m12", "keywords": ["docker compose", "mongodb"]},
    {"id": "c13", "query": "GOLDEN_13_EMBED_MODEL", "expect_key": "m13", "keywords": ["BGE-m3-ko", "임베딩"]},
    {"id": "c14", "query": "GOLDEN_14_LOCK_ENCODE", "expect_key": "m14", "keywords": ["encode lock", "스파이크"]},
    {"id": "c15", "query": "GOLDEN_15_STALE_POLICY", "expect_key": "m15", "keywords": ["stale", "14일"]},
    {"id": "c16", "query": "GOLDEN_16_REINDEX_CLEAR", "expect_key": "m16", "keywords": ["--clear", "centroid"]},
    {"id": "c17", "query": "GOLDEN_17_TOPIC_LOOKUP", "expect_key": "m17", "keywords": ["topic_lookup", "계층"]},
    {"id": "c18", "query": "GOLDEN_18_MEMORY_DELETE", "expect_key": "m18", "keywords": ["memory_count", "동기화"]},
    {"id": "c19", "query": "GOLDEN_19_SENSITIVITY", "expect_key": "m19", "keywords": ["숨김", "UX"], "category": "emotion"},
    {"id": "c20", "query": "GOLDEN_20_WORK_LOG", "expect_key": "m20", "keywords": ["work_log", "HANDOFF"]},
]


@pytest.fixture(autouse=True)
async def cleanup():
    db = get_db()
    await db["memories"].delete_many({"context": TEST_CONTEXT})
    yield
    await db["memories"].delete_many({"context": TEST_CONTEXT})


async def test_recall_golden_regression_set_20_cases():
    saved_ids: dict[str, str] = {}
    for key, row in GOLDEN_MEMORIES.items():
        oid = await save_memory(
            content=row["content"],
            category=row["category"],
            importance=row["importance"],
            context=TEST_CONTEXT,
            entities=[],
        )
        saved_ids[key] = str(oid)

    for case in GOLDEN_CASES:
        args = {
            "query": case["query"],
            "include_debug": True,
            "top_k": 5,
        }
        if "category" in case:
            args["category"] = case["category"]

        result = await handle("memory_recall", args)
        assert result is not None, case["id"]

        payload = json.loads(result[0].text)
        rows = payload.get("results", [])
        found_ids = {row.get("id") for row in rows}

        expected_id = saved_ids[case["expect_key"]]
        assert expected_id in found_ids, case["id"]

        content_blob = " ".join(str(row.get("content", "")) for row in rows)
        for keyword in case["keywords"]:
            assert keyword in content_blob, f"{case['id']} missing keyword: {keyword}"
