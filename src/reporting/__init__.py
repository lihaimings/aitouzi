from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional

import pandas as pd


def render_markdown_report(
    metrics: Dict[str, float],
    latest_weights: pd.Series,
    start_date: Optional[pd.Timestamp] = None,
    end_date: Optional[pd.Timestamp] = None,
) -> str:
    def pct(x: float) -> str:
        return f"{x * 100:.2f}%"

    period = ""
    if start_date is not None and end_date is not None:
        period = f"\n- 回测区间：{start_date.date()} ~ {end_date.date()}"

    active = latest_weights[latest_weights > 0].sort_values(ascending=False)
    if active.empty:
        pos_text = "- 当前空仓"
    else:
        pos_lines = [f"- {k}: {v:.2%}" for k, v in active.items()]
        pos_text = "\n".join(pos_lines)

    extra_lines = ""
    if "sortino" in metrics:
        extra_lines += f"- Sortino：{metrics.get('sortino', 0.0):.2f}\n"
    if "calmar" in metrics:
        extra_lines += f"- Calmar：{metrics.get('calmar', 0.0):.2f}\n"
    if "win_rate" in metrics:
        extra_lines += f"- 胜率（日频）：{metrics.get('win_rate', 0.0):.2%}\n"
    if "alpha_annual" in metrics:
        extra_lines += f"- 年化Alpha：{pct(metrics.get('alpha_annual', 0.0))}\n"
    if "cost_total" in metrics:
        extra_lines += f"- 交易总成本（权重口径）：{metrics.get('cost_total', 0.0):.4f}\n"
    if "cost_impact" in metrics:
        extra_lines += f"- 冲击成本（权重口径）：{metrics.get('cost_impact', 0.0):.4f}\n"
    if "avg_turnover" in metrics:
        extra_lines += f"- 平均换手（单日）：{metrics.get('avg_turnover', 0.0):.2%}\n"

    return (
        "# AI 投资策略日报（纸盘）\n\n"
        "## 绩效摘要\n"
        f"- 总收益：{pct(metrics.get('total_return', 0.0))}\n"
        f"- 年化收益：{pct(metrics.get('annual_return', 0.0))}\n"
        f"- 年化波动：{pct(metrics.get('annual_vol', 0.0))}\n"
        f"- Sharpe：{metrics.get('sharpe', 0.0):.2f}\n"
        f"- 最大回撤：{pct(metrics.get('max_drawdown', 0.0))}\n"
        f"{extra_lines}"
        f"{period}\n\n"
        "## 最新持仓\n"
        f"{pos_text}\n\n"
        "## 说明\n"
        "- 策略：ETF 轮动（20/60日动量），周频调仓\n"
        "- 约束：含手续费+滑点，支持换手上限与风险平价权重\n"
        "- 风险提示：历史回测不代表未来表现\n"
    )


def save_report(markdown_text: str, filename: str = "paper_daily_report.md") -> Path:
    out_dir = Path(__file__).resolve().parents[2] / "reports"
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / filename
    out.write_text(markdown_text, encoding="utf-8")
    return out
