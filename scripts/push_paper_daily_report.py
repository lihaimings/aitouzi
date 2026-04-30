from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.reporting.feishu_push import push_dm


def main():
    report_path = ROOT / "reports" / "paper_trade_daily_2026-03-18.md"
    if not report_path.exists():
        raise FileNotFoundError(f"report not found: {report_path}")
    text = report_path.read_text(encoding="utf-8").strip()
    push_dm(text)


if __name__ == "__main__":
    main()
