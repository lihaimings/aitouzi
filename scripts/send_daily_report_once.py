import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.reporting.feishu_push import push_dm


def main():
    report_path = ROOT / "reports" / "paper_trade_daily_2026-03-18.md"
    if not report_path.exists():
        raise FileNotFoundError(f"report not found: {report_path}")
    text = report_path.read_text(encoding="utf-8").strip()
    # 飞书文本消息有长度上限，做一次保护
    max_len = 3500
    if len(text) <= max_len:
        push_dm(text)
        return

    head = text[:max_len]
    tail = text[max_len:]
    push_dm(head)
    for i in range(0, len(tail), max_len):
        chunk = tail[i:i+max_len]
        push_dm(f"[续 {i//max_len + 1}]\n" + chunk)


if __name__ == "__main__":
    main()
