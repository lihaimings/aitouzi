from pathlib import Path
from src.reporting.feishu_push import push_dm

root = Path(__file__).resolve().parents[1]

daily = (root / "reports" / "paper_rotation_daily.md").read_text(encoding="utf-8")
risk = (root / "reports" / "paper_rotation_risk_guardrails.md").read_text(encoding="utf-8")
ai = (root / "reports" / "paper_rotation_ai_review.md").read_text(encoding="utf-8")

# 飞书文本长度控制，保留关键摘要
text = "\n".join([
    "【纸交易日报】A股/ETF（仅纸盘）",
    "",
    daily.strip(),
    "",
    "---",
    risk.strip(),
    "",
    "---",
    "【情绪/AI摘要】",
    ai.strip(),
])

if len(text) > 3900:
    text = text[:3800] + "\n\n（内容过长，已截断；完整版本见本地 reports/ 目录）"

push_dm(text)
print("manual daily report pushed")
