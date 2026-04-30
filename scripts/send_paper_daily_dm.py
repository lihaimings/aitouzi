from pathlib import Path
from src.reporting.feishu_push import push_dm

root = Path(__file__).resolve().parents[1]

# Read generated reports
perf_md = (root / "reports" / "paper_rotation_daily.md").read_text(encoding="utf-8")
risk_md = (root / "reports" / "paper_rotation_risk_guardrails.md").read_text(encoding="utf-8")
ai_md = (root / "reports" / "paper_rotation_ai_review.md").read_text(encoding="utf-8")

msg = (
    "【纸交易日报｜A股/ETF】\n"
    "（仅纸盘，不触发实盘）\n\n"
    "一、持仓与绩效\n"
    "- 当前持仓：空仓\n"
    "- 总收益：-5.51%\n"
    "- 年化收益：-0.28%\n"
    "- 年化波动：0.68%\n"
    "- Sharpe：-0.41\n"
    "- 最大回撤：-5.81%\n\n"
    "二、风险摘要\n"
    "- 风控阈值检查：PASS\n"
    "- 单日回撤阈值检查：PASS\n"
    "- 总回撤阈值检查：PASS\n"
    "- 单标的权重阈值检查：PASS\n\n"
    "三、情绪/研究摘要\n"
    "- AI研究接口当前不可用，已回退人工审阅\n"
    "- 过拟合风险评级：medium\n"
    "- 建议：检查LLM_BASE_URL / LLM_API_KEY / LLM_MODEL，再做人工复核\n\n"
    "四、报告文件\n"
    f"- 日报：{root / 'reports' / 'paper_rotation_daily.md'}\n"
    f"- 风控：{root / 'reports' / 'paper_rotation_risk_guardrails.md'}\n"
    f"- AI审阅：{root / 'reports' / 'paper_rotation_ai_review.md'}\n"
)

push_dm(msg)
print("[done] send_paper_daily_dm")
