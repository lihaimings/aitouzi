import json
from datetime import datetime
from pathlib import Path

BASE = Path(__file__).resolve().parents[2]


def load(path: Path):
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def main():
    minute = load(BASE / "state" / "minute_history_status.json")
    macro = load(BASE / "state" / "macro_history_3y_status.json")
    msum = minute.get("summary", {})
    xsum = macro.get("summary", {})

    lines = [
        "# Aitouzi 历史数据覆盖率报告",
        "",
        f"生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "## 分钟级历史",
        f"- 状态：{'OK' if msum.get('ok') else 'FAIL'}",
        f"- 成功率：{msum.get('ok_count')}/{msum.get('total')} ({msum.get('success_rate')})",
        "",
        "## 宏观3年历史",
        f"- 状态：{'OK' if xsum.get('ok') else 'FAIL'}",
        f"- 序列成功率：{xsum.get('series_ok')}/{xsum.get('series_total')} ({xsum.get('success_rate')})",
        f"- 合并行数：{xsum.get('merged_rows')}",
        f"- 覆盖范围：{xsum.get('merged_min_date')} -> {xsum.get('merged_max_date')}",
        "",
    ]

    out = BASE / "reports" / "quant_history_coverage.md"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(lines), encoding="utf-8")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
