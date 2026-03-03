from __future__ import annotations

from pathlib import Path
from typing import Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd

from src.backtest.vectorbt_runner import load_close_matrix

REPORT_DIR = Path(__file__).resolve().parents[2] / "reports"


def _annual_return_from_series(ret: pd.Series, trading_days: int = 252) -> float:
    ret = pd.Series(ret).fillna(0.0)
    if len(ret) == 0:
        return 0.0
    equity = (1.0 + ret).cumprod()
    total = float(equity.iloc[-1] / equity.iloc[0] - 1.0)
    return float((1.0 + total) ** (trading_days / max(len(ret), 1)) - 1.0)


def compare_against_benchmarks(
    strategy_returns: pd.Series,
    benchmark_codes: Iterable[str],
    source: str = "baostock",
    trading_days: int = 252,
) -> pd.DataFrame:
    strategy_returns = pd.Series(strategy_returns).dropna().copy()
    strategy_returns.name = "strategy"

    rows: List[dict] = []
    for code in benchmark_codes:
        try:
            close = load_close_matrix([code], source=source)
            bench_ret = close[code].pct_change().fillna(0.0).reindex(strategy_returns.index).fillna(0.0)

            strat_ann = _annual_return_from_series(strategy_returns, trading_days=trading_days)
            bench_ann = _annual_return_from_series(bench_ret, trading_days=trading_days)

            active = strategy_returns - bench_ret
            alpha_annual = float(active.mean() * trading_days)
            tracking_error_annual = float(active.std(ddof=0) * np.sqrt(trading_days))
            information_ratio = float(alpha_annual / tracking_error_annual) if tracking_error_annual > 0 else 0.0
            active_win_rate = float((active > 0).mean())

            rows.append(
                {
                    "benchmark_code": code,
                    "strategy_annual_return": strat_ann,
                    "benchmark_annual_return": bench_ann,
                    "alpha_annual": alpha_annual,
                    "tracking_error_annual": tracking_error_annual,
                    "information_ratio": information_ratio,
                    "active_win_rate": active_win_rate,
                }
            )
        except Exception as e:
            rows.append(
                {
                    "benchmark_code": code,
                    "strategy_annual_return": np.nan,
                    "benchmark_annual_return": np.nan,
                    "alpha_annual": np.nan,
                    "tracking_error_annual": np.nan,
                    "information_ratio": np.nan,
                    "active_win_rate": np.nan,
                    "error": str(e),
                }
            )

    out = pd.DataFrame(rows)
    if out.empty:
        return out
    return out.sort_values("information_ratio", ascending=False, na_position="last").reset_index(drop=True)


def _render_benchmark_markdown(df: pd.DataFrame) -> str:
    if df.empty:
        return "# 多基准对照报告\n\n- 无可用基准结果\n"

    lines = [
        "# 多基准对照报告\n",
        "## 指标说明\n",
        "- alpha_annual：策略相对基准的年化超额收益\n",
        "- tracking_error_annual：超额收益年化波动\n",
        "- information_ratio(IR)：alpha_annual / tracking_error_annual\n",
        "## 对照结果\n",
        "| benchmark | strategy_ann | bench_ann | alpha_ann | TE_ann | IR | active_win_rate |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]

    for _, r in df.iterrows():
        if pd.notna(r.get("error")):
            lines.append(f"| {r['benchmark_code']} | - | - | - | - | - | - |")
            continue
        lines.append(
            f"| {r['benchmark_code']} | {float(r['strategy_annual_return']):.4f} | "
            f"{float(r['benchmark_annual_return']):.4f} | {float(r['alpha_annual']):.4f} | "
            f"{float(r['tracking_error_annual']):.4f} | {float(r['information_ratio']):.3f} | "
            f"{float(r['active_win_rate']):.2%} |"
        )

    return "\n".join(lines) + "\n"


def save_benchmark_compare_outputs(
    df: pd.DataFrame,
    prefix: str = "paper_rotation",
) -> Tuple[Path, Path]:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = REPORT_DIR / f"{prefix}_benchmark_compare.csv"
    md_path = REPORT_DIR / f"{prefix}_benchmark_compare.md"

    df.to_csv(csv_path, index=False)
    md_path.write_text(_render_benchmark_markdown(df), encoding="utf-8")
    return csv_path, md_path
