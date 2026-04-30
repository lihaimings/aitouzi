from pathlib import Path
from src.reporting.feishu_push import push_dm

root = Path(__file__).resolve().parents[1]
report_path = root / "reports" / "paper_rotation_daily.md"
if not report_path.exists():
    raise SystemExit(f"daily report not found: {report_path}")
text = report_path.read_text(encoding="utf-8")
push_dm(text)
print("pushed daily report")
