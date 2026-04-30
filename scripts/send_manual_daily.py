from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.reporting.feishu_push import push_dm


def main() -> None:
    report_path = ROOT / "reports" / "paper_rotation_daily.md"
    if not report_path.exists():
        raise SystemExit(f"missing report: {report_path}")

    text = report_path.read_text(encoding="utf-8")

    # 提炼成飞书可读摘要（控制长度）
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    msg = (
        f"【纸交易日报】{now}\n"
        f"市场：A股/ETF（仅纸盘，不触发实盘）\n\n"
        f"{text}\n\n"
        f"附：完整报告文件 {report_path}"
    )

    # 飞书文本消息建议不要过长
    if len(msg) > 3500:
        msg = msg[:3400] + "\n\n（内容已截断，请查看本地完整报告文件）"

    push_dm(msg)


if __name__ == "__main__":
    main()
