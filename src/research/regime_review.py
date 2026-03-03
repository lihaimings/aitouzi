from __future__ import annotations

from pathlib import Path
from typing import Dict, Tuple

import pandas as pd

REPORT_DIR = Path(__file__).resolve().parents[2] / "reports"


def build_regime_review(
    strategy_returns: pd.Series,
    benchmark_returns: pd.Series,
    lookback_days: int = 20,
    bull_threshold: float = 0.03,
    bear_threshold: float = -0.03,
) -> pd.DataFrame:
    strategy_returns = pd.Series(strategy_returns).fillna(0.0)
    benchmark_returns = pd.Series(benchmark_returns).reindex(strategy_returns.index).fillna(0.0)

    regime_signal = (1.0 + benchmark_returns).rolling(lookback_days).apply(lambda x: x.prod() - 1.0, raw=False)

    regime = pd.Series("sideways", index=strategy_returns.index, dtype="object")
    regime.loc[regime_signal >= bull_threshold] = "bull"
    regime.loc[regime_signal <= bear_threshold] = "bear"

    frame = pd.DataFrame(
        {
            "date": strategy_returns.index,
            "strategy_ret": strategy_returns.values,
            "benchmark_ret": benchmark_returns.values,
            "regime": regime.values,
        }
    )
    frame = frame.dropna(subset=["date"]).copy()
    if frame.empty:
        return pd.DataFrame()

    rows = []
    for name, grp in frame.groupby("regime"):
        if grp.empty:
            continue
        strat_eq = (1.0 + grp["strategy_ret"]).cumprod()
        bench_eq = (1.0 + grp["benchmark_ret"]).cumprod()
        rows.append(
            {
                "regime": name,
                "days": int(len(grp)),
                "strategy_return": float(strat_eq.iloc[-1] - 1.0),
                "benchmark_return": float(bench_eq.iloc[-1] - 1.0),
                "excess_return": float((strat_eq.iloc[-1] - 1.0) - (bench_eq.iloc[-1] - 1.0)),
                "strategy_win_rate": float((grp["strategy_ret"] > 0).mean()),
                "benchmark_win_rate": float((grp["benchmark_ret"] > 0).mean()),
            }
        )

    out = pd.DataFrame(rows)
    if out.empty:
        return out
    return out.sort_values("days", ascending=False).reset_index(drop=True)


def _render_regime_markdown(df: pd.DataFrame) -> str:
    if df.empty:
        return "# 市场阶段复盘\n\n- 无可用数据\n"

    lines = [
        "# 市场阶段复盘（自动生成）\n",
        "| regime | days | strategy_return | benchmark_return | excess_return | strategy_win_rate | benchmark_win_rate |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for _, r in df.iterrows():
        lines.append(
            "| "
            f"{r['regime']} | {int(r['days'])} | {float(r['strategy_return']):.2%} | {float(r['benchmark_return']):.2%} | "
            f"{float(r['excess_return']):.2%} | {float(r['strategy_win_rate']):.2%} | {float(r['benchmark_win_rate']):.2%} |"
        )
    lines.append("\n- 注：regime 基于基准近N日累计收益简单分段，仅作研究参考。")
    return "\n".join(lines) + "\n"


def save_regime_review(df: pd.DataFrame, prefix: str = "paper_rotation") -> Tuple[Path, Path]:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = REPORT_DIR / f"{prefix}_regime_review.csv"
    md_path = REPORT_DIR / f"{prefix}_regime_review.md"

    df.to_csv(csv_path, index=False)
    md_path.write_text(_render_regime_markdown(df), encoding="utf-8")
    return csv_path, md_path


def pick_regime_key_insight(df: pd.DataFrame) -> Dict:
    if df.empty:
        return {}
    best = df.sort_values("excess_return", ascending=False).iloc[0].to_dict()
    worst = df.sort_values("excess_return", ascending=True).iloc[0].to_dict()
    return {"best_regime": best, "worst_regime": worst}
