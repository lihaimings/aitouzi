#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.strategy import classify_etf_frame, load_class_config, load_sample_universe, summarize_classification


def main() -> int:
    parser = argparse.ArgumentParser(description="Run ETF strategy classification snapshot")
    parser.add_argument("--input-csv", default="", help="Optional ETF universe csv")
    parser.add_argument("--out-csv", default=str(ROOT / "reports" / "etf_strategy_classification_snapshot.csv"))
    parser.add_argument("--out-json", default=str(ROOT / "reports" / "etf_strategy_classification_summary.json"))
    parser.add_argument("--out-md", default=str(ROOT / "reports" / "etf_strategy_classification_snapshot.md"))
    args = parser.parse_args()

    input_path = Path(args.input_csv) if args.input_csv else None
    df = load_sample_universe(input_path)
    cfg = load_class_config()
    classified = classify_etf_frame(df, config=cfg)
    summary = summarize_classification(classified)

    out_csv = Path(args.out_csv)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    classified.to_csv(out_csv, index=False, encoding="utf-8-sig")

    out_json = Path(args.out_json)
    out_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [
        "# ETF分型快照",
        "",
        f"- 总数：{summary['total']}",
        "- 分类统计：",
    ]
    for klass, count in summary["class_counts"].items():
        lines.append(f"  - `{klass}`: {count}")
    lines.extend(["", "## 样本", "", classified.head(20).to_markdown(index=False)])
    Path(args.out_md).write_text("\n".join(lines), encoding="utf-8")

    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
