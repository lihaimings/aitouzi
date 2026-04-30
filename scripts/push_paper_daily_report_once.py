import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.reporting.feishu_push import push_dm

REPORT_PATH = ROOT / "reports" / "paper_trade_daily_2026-03-18.md"


def main():
    if not REPORT_PATH.exists():
        raise SystemExit(f"report not found: {REPORT_PATH}")
    text = REPORT_PATH.read_text(encoding="utf-8")
    push_dm(text)
    print("push_done")


if __name__ == "__main__":
    main()
