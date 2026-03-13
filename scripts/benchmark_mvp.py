"""
Simple MVP benchmark for low-spec environments.

Measures:
- memory_save latency
- memory_recall latency
- approximate peak RSS change
"""
import argparse
import asyncio
import json
import resource
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src import config
from src.db.connection import get_db, close_connection
from src.tools.memory_save import handle as memory_save_handle
from src.tools.memory_recall import handle as memory_recall_handle

BENCH_CONTEXT = "benchmark_mvp_ctx"
BENCH_QUERY_TOKEN = "benchmark_mvp_token"


def _percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, int((len(ordered) - 1) * p)))
    return ordered[idx]


def _summary(values: list[float]) -> dict:
    if not values:
        return {"count": 0, "avg_ms": 0.0, "p50_ms": 0.0, "p95_ms": 0.0, "max_ms": 0.0}
    return {
        "count": len(values),
        "avg_ms": round(sum(values) / len(values), 2),
        "p50_ms": round(_percentile(values, 0.50), 2),
        "p95_ms": round(_percentile(values, 0.95), 2),
        "max_ms": round(max(values), 2),
    }


async def _cleanup() -> None:
    db = get_db()
    await db["memories"].delete_many({"context": BENCH_CONTEXT})
    await db["topics"].delete_many({"title": {"$regex": "benchmark_mvp"}})


async def _seed(seed_count: int) -> None:
    for i in range(seed_count):
        await memory_save_handle("memory_save", {
            "content": f"{BENCH_QUERY_TOKEN} seed memory {i}",
            "category": "fact",
            "importance": 5,
            "context": BENCH_CONTEXT,
            "entities": [],
        })


async def _measure_save(runs: int) -> list[float]:
    latencies: list[float] = []
    for i in range(runs):
        start = time.perf_counter()
        await memory_save_handle("memory_save", {
            "content": f"{BENCH_QUERY_TOKEN} save run {i}",
            "category": "fact",
            "importance": 5,
            "context": BENCH_CONTEXT,
            "entities": [],
        })
        latencies.append((time.perf_counter() - start) * 1000.0)
    return latencies


async def _measure_recall(runs: int) -> list[float]:
    latencies: list[float] = []
    for _ in range(runs):
        start = time.perf_counter()
        await memory_recall_handle("memory_recall", {
            "query": BENCH_QUERY_TOKEN,
            "top_k": 5,
            "time_range": "all",
        })
        latencies.append((time.perf_counter() - start) * 1000.0)
    return latencies


async def main(args: argparse.Namespace) -> None:
    rss_before_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss

    await _cleanup()
    await _seed(args.seed)

    # warmup
    for _ in range(args.warmup):
        await memory_save_handle("memory_save", {
            "content": f"{BENCH_QUERY_TOKEN} warmup save",
            "category": "fact",
            "importance": 4,
            "context": BENCH_CONTEXT,
            "entities": [],
        })
        await memory_recall_handle("memory_recall", {
            "query": BENCH_QUERY_TOKEN,
            "top_k": 3,
            "time_range": "all",
        })

    save_ms = await _measure_save(args.runs_save)
    recall_ms = await _measure_recall(args.runs_recall)

    rss_after_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss

    report = {
        "context": BENCH_CONTEXT,
        "seed_count": args.seed,
        "warmup": args.warmup,
        "chroma_enabled": config.CHROMA_ENABLED,
        "chroma_collection_name": config.CHROMA_COLLECTION_NAME,
        "embedding_model_name": config.EMBEDDING_MODEL_NAME,
        "save_latency": _summary(save_ms),
        "recall_latency": _summary(recall_ms),
        "rss_max_mb_before": round(rss_before_kb / 1024.0, 2),
        "rss_max_mb_after": round(rss_after_kb / 1024.0, 2),
        "rss_max_mb_increase": round((rss_after_kb - rss_before_kb) / 1024.0, 2),
    }

    print(json.dumps(report, ensure_ascii=False, indent=2))

    if not args.keep_data:
        await _cleanup()
    await close_connection()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MVP benchmark (memory_save/memory_recall)")
    parser.add_argument("--seed", type=int, default=50, help="seed memory count")
    parser.add_argument("--warmup", type=int, default=3, help="warmup iterations")
    parser.add_argument("--runs-save", type=int, default=20, help="save benchmark iterations")
    parser.add_argument("--runs-recall", type=int, default=20, help="recall benchmark iterations")
    parser.add_argument("--keep-data", action="store_true", help="keep benchmark data after run")
    asyncio.run(main(parser.parse_args()))
