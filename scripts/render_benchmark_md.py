"""
Render a benchmark_mvp JSON report as Markdown tables.

Usage:
  python scripts/render_benchmark_md.py \
    --input docs/benchmarks/mvp_latest.json \
    --output docs/benchmarks/mvp_latest.md
"""
import argparse
import json
from pathlib import Path


def _fmt(value: object) -> str:
    if isinstance(value, float):
        return f"{value:.2f}"
    return str(value)


def _table_rows(section: dict) -> list[str]:
    return [
        f"| count | {_fmt(section.get('count', 0))} |",
        f"| avg_ms | {_fmt(section.get('avg_ms', 0.0))} |",
        f"| p50_ms | {_fmt(section.get('p50_ms', 0.0))} |",
        f"| p95_ms | {_fmt(section.get('p95_ms', 0.0))} |",
        f"| max_ms | {_fmt(section.get('max_ms', 0.0))} |",
    ]


def main() -> None:
    parser = argparse.ArgumentParser(description="Render benchmark JSON to markdown tables.")
    parser.add_argument("--input", required=True, help="benchmark JSON path")
    parser.add_argument("--output", required=True, help="markdown output path")
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)
    payload = json.loads(input_path.read_text(encoding="utf-8"))

    save_section = payload.get("save_latency", {})
    recall_section = payload.get("recall_latency", {})

    lines = [
        "# MVP Benchmark Snapshot",
        "",
        f"- context: `{payload.get('context', '')}`",
        f"- seed_count: `{payload.get('seed_count', 0)}`",
        f"- warmup: `{payload.get('warmup', 0)}`",
        f"- chroma_enabled: `{payload.get('chroma_enabled', False)}`",
        f"- chroma_collection_name: `{payload.get('chroma_collection_name', '')}`",
        f"- embedding_model_name: `{payload.get('embedding_model_name', '')}`",
        "",
        "## save_latency",
        "",
        "| metric | value |",
        "|---|---:|",
        *_table_rows(save_section),
        "",
        "## recall_latency",
        "",
        "| metric | value |",
        "|---|---:|",
        *_table_rows(recall_section),
        "",
        "## memory_usage",
        "",
        "| metric | value_mb |",
        "|---|---:|",
        f"| rss_max_mb_before | {_fmt(payload.get('rss_max_mb_before', 0.0))} |",
        f"| rss_max_mb_after | {_fmt(payload.get('rss_max_mb_after', 0.0))} |",
        f"| rss_max_mb_increase | {_fmt(payload.get('rss_max_mb_increase', 0.0))} |",
        "",
    ]

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines), encoding="utf-8")


if __name__ == "__main__":
    main()
