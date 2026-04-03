#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.strategy import build_monitor_template, classify_etf_frame, load_class_config, load_sample_universe


def main() -> int:
    parser = argparse.ArgumentParser(description="Run minimal ETF class monitor snapshot")
    parser.add_argument("--out-json", default=str(ROOT / "reports" / "etf_class_monitor_snapshot.json"))
    parser.add_argument("--out-md", default=str(ROOT / "reports" / "etf_class_monitor_snapshot.md"))
    args = parser.parse_args()

    df = classify_etf_frame(load_sample_universe(), config=load_class_config())
    grouped = df.groupby("strategy_class", dropna=False).head(1)

    payload = {}
    for _, row in grouped.iterrows():
        klass = str(row["strategy_class"])
        monitor = build_monitor_template(str(row["monitor_template"]))
        payload[klass] = {
            "sample_code": row.get("code"),
            "sample_name": row.get("name"),
            "monitor_template": row.get("monitor_template"),
            "monitor_focus": monitor.get("focus", []),
            "cadence": monitor.get("cadence"),
        }

    out_json = Path(args.out_json)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = ["# 各分型最小可用监控模板", ""]
    for klass, item in payload.items():
        lines.append(f"## {klass}")
        lines.append(f"- 样本：`{item['sample_code']}` {item['sample_name']}")
        lines.append(f"- 模板：`{item['monitor_template']}`")
        lines.append(f"- 节奏：`{item['cadence']}`")
        lines.append(f"- 关注点：{', '.join(item['monitor_focus'])}")
        lines.append("")
    Path(args.out_md).write_text("\n".join(lines), encoding="utf-8")

    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
