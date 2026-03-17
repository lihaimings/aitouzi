import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.reporting.feishu_push import push_dm


def main():
    reports = ROOT / "reports"
    daily = (reports / "paper_rotation_daily.md").read_text(encoding="utf-8") if (reports / "paper_rotation_daily.md").exists() else "(日报缺失)"
    risk = (reports / "paper_rotation_risk_guardrails.md").read_text(encoding="utf-8") if (reports / "paper_rotation_risk_guardrails.md").exists() else "(风控报告缺失)"
    regime = (reports / "paper_rotation_regime_review.md").read_text(encoding="utf-8") if (reports / "paper_rotation_regime_review.md").exists() else "(情绪/市场阶段报告缺失)"

    text = "\n\n".join([
        "【纸交易日报 | A股/ETF | 仅纸盘】",
        daily,
        "【风险摘要】\n" + risk,
        "【情绪/市场阶段摘要】\n" + regime,
        "注：本次仅生成并推送纸盘日报，未触发任何实盘交易。",
    ])

    push_dm(text)


if __name__ == "__main__":
    main()
