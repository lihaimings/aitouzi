from pathlib import Path
from src.reporting.feishu_push import push_dm


def main():
    report_dir = Path(__file__).resolve().parents[1] / "reports"
    candidates = sorted(report_dir.glob("paper_trade_daily_*.md"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not candidates:
        raise SystemExit("no paper_trade_daily report found")
    latest = candidates[0]
    text = latest.read_text(encoding="utf-8")
    push_dm(text)
    print(f"pushed: {latest}")


if __name__ == "__main__":
    main()
