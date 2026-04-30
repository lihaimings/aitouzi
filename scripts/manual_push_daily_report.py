from pathlib import Path
from src.reporting.feishu_push import push_dm

root = Path(__file__).resolve().parents[1]

daily = (root / "reports" / "paper_rotation_daily.md").read_text(encoding="utf-8")
ai = (root / "reports" / "paper_rotation_ai_review.md").read_text(encoding="utf-8")
risk = (root / "reports" / "paper_rotation_risk_guardrails.md").read_text(encoding="utf-8")

msg = (
    "【纸交易日报｜A股/ETF】\n"
    "（仅纸盘，不触发实盘）\n\n"
    + daily
    + "\n\n"
    + ai
    + "\n\n"
    + risk
)

push_dm(msg)
print("manual daily report push done")
