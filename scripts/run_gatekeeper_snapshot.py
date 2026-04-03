#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.strategy import (
    build_gatekeeper_metrics,
    load_gatekeeper_config,
    save_gatekeeper_markdown,
    save_gatekeeper_snapshot,
    score_gatekeeper,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run ETF red-yellow-green gatekeeper snapshot")
    parser.add_argument("--metrics-json", help="Inline JSON metrics payload", default="")
    parser.add_argument("--metrics-file", help="Path to JSON metrics payload", default="")
    parser.add_argument("--out-json", default=str(ROOT / "reports" / "gatekeeper_snapshot.json"))
    parser.add_argument("--out-md", default=str(ROOT / "reports" / "gatekeeper_snapshot.md"))
    args = parser.parse_args()

    raw_metrics = {}
    if args.metrics_file:
        raw_metrics = json.loads(Path(args.metrics_file).read_text(encoding="utf-8"))
    elif args.metrics_json:
        raw_metrics = json.loads(args.metrics_json)
    else:
        raw_metrics = {
            "macro_risk": 0.58,
            "drawdown_risk": 0.42,
            "breadth_risk": 0.47,
            "volatility_risk": 0.51,
        }

    metrics = build_gatekeeper_metrics(raw_metrics)
    config = load_gatekeeper_config()
    result = score_gatekeeper(metrics, config=config)
    save_gatekeeper_snapshot(result, Path(args.out_json))
    save_gatekeeper_markdown(result, Path(args.out_md))

    print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
