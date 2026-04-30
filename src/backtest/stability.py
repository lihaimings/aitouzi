from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd

from src.backtest.vectorbt_runner import load_close_matrix, run_rotation_backtest

REPORT_DIR = Path(__file__).resolve().parents[2] / "reports"


def run_parameter_stability_from_local_cache(
    codes: Iterable[str],
    source: str = "baostock",
    rebalance: str = "W-FRI",
    fee_bps: float = 5.0,
    slippage_bps: float = 5.0,
    top_n_grid: Optional[List[int]] = None,
    min_score_grid: Optional[List[float]] = None,
    vol_lookback_grid: Optional[List[int]] = None,
    benchmark_code: Optional[str] = None,
    max_turnover: Optional[float] = 0.8,
    use_risk_parity: bool = True,
    asset_params: Optional[Dict[str, Dict[str, float]]] = None,
) -> pd.DataFrame:
    close = load_close_matrix(codes=codes, source=source)

    bench = None
    if benchmark_code is not None and benchmark_code in close.columns:
        bench = close[benchmark_code].pct_change().fillna(0.0)

    top_n_grid = top_n_grid or [1, 2, 3]
    min_score_grid = min_score_grid or [-0.2, -0.1, 0.0]
    vol_lookback_grid = vol_lookback_grid or [10, 20, 40]

    rows: List[Dict[str, float]] = []
    for top_n in top_n_grid:
        for min_score in min_score_grid:
            for vol_lookback in vol_lookback_grid:
                res = run_rotation_backtest(
                    close_df=close,
                    rebalance=rebalance,
                    top_n=top_n,
                    fee_bps=fee_bps,
                    slippage_bps=slippage_bps,
                    min_score=min_score,
                    benchmark_returns=bench,
                    max_turnover=max_turnover,
                    use_risk_parity=use_risk_parity,
                    vol_lookback=vol_lookback,
                    asset_params=asset_params,
                )
                m = res.metrics
                objective = float(m.get("annual_return", 0.0) + 0.3 * m.get("max_drawdown", 0.0))
                rows.append(
                    {
                        "top_n": int(top_n),
                        "min_score": float(min_score),
                        "vol_lookback": int(vol_lookback),
                        "annual_return": float(m.get("annual_return", 0.0)),
                        "sharpe": float(m.get("sharpe", 0.0)),
                        "sortino": float(m.get("sortino", 0.0)),
                        "max_drawdown": float(m.get("max_drawdown", 0.0)),
                        "calmar": float(m.get("calmar", 0.0)),
                        "alpha_annual": float(m.get("alpha_annual", 0.0)),
                        "objective": objective,
                    }
                )

    out = pd.DataFrame(rows).sort_values("objective", ascending=False).reset_index(drop=True)
    return out


def _render_stability_markdown(df: pd.DataFrame) -> str:
    if df.empty:
        return "# 参数稳定性报告\n\n- 无结果\n"

    top = df.head(10).copy()
    lines = [
        "# 参数稳定性报告\n",
        "## 说明\n",
        "- 目标函数：annual_return + 0.3 * max_drawdown\n",
        "- max_drawdown 为负值，因此更大 objective 代表收益回撤权衡更优\n",
        "## Top 10 参数组合\n",
        "| rank | top_n | min_score | vol_lookback | annual_return | sharpe | max_drawdown | calmar | alpha_annual | objective |",
        "|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]

    for i, r in top.iterrows():
        lines.append(
            f"| {i+1} | {int(r['top_n'])} | {float(r['min_score']):.2f} | {int(r['vol_lookback'])} | "
            f"{float(r['annual_return']):.4f} | {float(r['sharpe']):.3f} | {float(r['max_drawdown']):.4f} | "
            f"{float(r['calmar']):.3f} | {float(r['alpha_annual']):.4f} | {float(r['objective']):.4f} |"
        )

    return "\n".join(lines) + "\n"


def save_parameter_stability_outputs(
    df: pd.DataFrame,
    prefix: str = "paper_rotation",
) -> Tuple[Path, Path, List[Path], List[Path]]:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    all_csv = REPORT_DIR / f"{prefix}_param_stability.csv"
    md_path = REPORT_DIR / f"{prefix}_param_stability.md"
    df.to_csv(all_csv, index=False)
    md_path.write_text(_render_stability_markdown(df), encoding="utf-8")

    pivot_csv_paths: List[Path] = []
    heatmap_png_paths: List[Path] = []

    if not df.empty:
        for v in sorted(df["vol_lookback"].unique().tolist()):
            sub = df[df["vol_lookback"] == v]
            pivot = sub.pivot(index="min_score", columns="top_n", values="objective").sort_index()
            pivot_csv = REPORT_DIR / f"{prefix}_param_stability_obj_vol{int(v)}.csv"
            pivot.to_csv(pivot_csv)
            pivot_csv_paths.append(pivot_csv)

            try:
                import matplotlib.pyplot as plt

                fig = plt.figure(figsize=(6, 4))
                ax = fig.add_subplot(111)
                im = ax.imshow(pivot.values, aspect="auto")
                ax.set_title(f"Objective Heatmap (vol_lookback={int(v)})")
                ax.set_xlabel("top_n")
                ax.set_ylabel("min_score")
                ax.set_xticks(range(len(pivot.columns)))
                ax.set_xticklabels([str(c) for c in pivot.columns])
                ax.set_yticks(range(len(pivot.index)))
                ax.set_yticklabels([f"{x:.2f}" for x in pivot.index])
                fig.colorbar(im, ax=ax)
                fig.tight_layout()

                png_path = REPORT_DIR / f"{prefix}_param_stability_obj_vol{int(v)}.png"
                fig.savefig(png_path, dpi=150)
                plt.close(fig)
                heatmap_png_paths.append(png_path)
            except Exception:
                pass

    return all_csv, md_path, pivot_csv_paths, heatmap_png_paths
