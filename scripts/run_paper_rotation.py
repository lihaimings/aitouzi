import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.backtest.vectorbt_runner import (
    run_from_local_cache,
    run_walk_forward_from_local_cache,
    save_backtest_outputs,
)
from src.data_pipeline import audit_universe, save_quality_reports
from src.backtest.backtesting_py_runner import run_backtestingpy_sma
from src.backtest.stability import (
    run_parameter_stability_from_local_cache,
    save_parameter_stability_outputs,
)
from src.backtest.benchmark_compare import (
    compare_against_benchmarks,
    save_benchmark_compare_outputs,
)
from src.research import build_research_recommendation, save_research_recommendation
from src.paper_trade import fills_to_frame, simulate_paper_trades
from src.reporting import render_markdown_report, save_report
from src.reporting.feishu_push import push_dm
from src.reporting.quantstats_report import generate_quantstats_html

DEFAULT_CODES = ["510300", "159915"]


def _discover_codes(source: str = "baostock"):
    discovered = []

    # 兼容两种命名：
    # 1) etf_510300_baostock.csv
    # 2) etf_510300.csv
    source_files = list((ROOT / "data").glob(f"etf_*_{source}.csv"))
    generic_files = list((ROOT / "data").glob("etf_*.csv"))

    for f in source_files + generic_files:
        parts = f.stem.split("_")
        if len(parts) >= 2 and parts[1].isdigit() and len(parts[1]) == 6:
            discovered.append(parts[1])

    discovered = sorted(set(discovered))
    return discovered if discovered else DEFAULT_CODES


def _try_generate_quantstats(result):
    try:
        return generate_quantstats_html(
            result.daily_returns,
            output_filename="paper_rotation_quantstats.html",
            benchmark=result.benchmark_returns,
        )
    except Exception as e:
        print(f"[warn] quantstats报告生成失败: {e}")
        return None


def _try_run_backtestingpy_baseline(code: str):
    try:
        return run_backtestingpy_sma(code=code, source="baostock", fast=10, slow=30, commission=0.001)
    except Exception as e:
        print(f"[warn] backtesting.py基准策略运行失败: {e}")
        return None


def _load_approved_params(prefix: str = "paper_rotation"):
    approved_path = ROOT / "reports" / f"{prefix}_approved_params.json"
    if not approved_path.exists():
        return {}, approved_path
    try:
        data = json.loads(approved_path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return {}, approved_path
        return data, approved_path
    except Exception:
        return {}, approved_path


def main():
    codes = _discover_codes(source="baostock")
    benchmark_code = codes[0]

    approved_params, approved_path = _load_approved_params(prefix="paper_rotation")

    exec_params = {
        "top_n": int(approved_params.get("top_n", 2)),
        "min_score": float(approved_params.get("min_score", -0.1)),
        "vol_lookback": int(approved_params.get("vol_lookback", 20)),
        "max_turnover": float(approved_params.get("max_turnover", 0.8)),
        "target_vol_ann": float(approved_params.get("target_vol_ann", 0.12)),
        "drawdown_stop": float(approved_params.get("drawdown_stop", -0.05)),
        "dd_cooldown_days": int(approved_params.get("dd_cooldown_days", 5)),
    }

    quality_df = audit_universe(codes=codes, source="baostock", jump_threshold=0.12)
    quality_csv_path, quality_md_path = save_quality_reports(quality_df, prefix="paper_rotation")

    result = run_from_local_cache(
        codes=codes,
        source="baostock",
        rebalance="W-FRI",
        top_n=exec_params["top_n"],
        fee_bps=5.0,
        slippage_bps=5.0,
        min_score=exec_params["min_score"],
        benchmark_code=benchmark_code,
        max_turnover=exec_params["max_turnover"],
        use_risk_parity=True,
        vol_lookback=exec_params["vol_lookback"],
        target_vol_ann=exec_params["target_vol_ann"],
        vol_target_lookback=20,
        max_leverage=1.0,
        drawdown_stop=exec_params["drawdown_stop"],
        dd_cooldown_days=exec_params["dd_cooldown_days"],
    )

    eq_path, wt_path = save_backtest_outputs(result, prefix="paper_rotation")

    wf_table, wf_returns = run_walk_forward_from_local_cache(
        codes=codes,
        source="baostock",
        rebalance="W-FRI",
        fee_bps=5.0,
        slippage_bps=5.0,
        train_days=15,
        test_days=8,
        step_days=8,
        top_n_grid=[1, 2],
        min_score_grid=[-0.2, -0.1, 0.0],
    )
    wf_table_path = ROOT / "reports" / "paper_rotation_walk_forward.csv"
    wf_equity_path = ROOT / "reports" / "paper_rotation_walk_forward_equity.csv"
    wf_table.to_csv(wf_table_path, index=False)
    ((1.0 + wf_returns).cumprod().rename("wf_equity")).to_csv(wf_equity_path, index=True)

    stability_df = run_parameter_stability_from_local_cache(
        codes=codes,
        source="baostock",
        rebalance="W-FRI",
        fee_bps=5.0,
        slippage_bps=5.0,
        top_n_grid=[1, 2, 3],
        min_score_grid=[-0.2, -0.1, 0.0],
        vol_lookback_grid=[10, 20, 40],
        benchmark_code=benchmark_code,
        max_turnover=0.8,
        use_risk_parity=True,
    )
    stability_csv_path, stability_md_path, stability_pivot_csvs, stability_heatmaps = save_parameter_stability_outputs(
        stability_df,
        prefix="paper_rotation",
    )

    recommendation = build_research_recommendation(
        stability_df=stability_df,
        wf_table=wf_table,
        current_params={
            "top_n": exec_params["top_n"],
            "min_score": exec_params["min_score"],
            "vol_lookback": exec_params["vol_lookback"],
        },
        min_pass_ratio=0.6,
        min_avg_sharpe=0.3,
        min_avg_annual_return=0.0,
        max_allowed_drawdown=-0.12,
    )
    rec_json_path, rec_md_path = save_research_recommendation(recommendation, prefix="paper_rotation")

    if result.exposure_scale is not None:
        exposure_path = ROOT / "reports" / "paper_rotation_exposure_scale.csv"
        result.exposure_scale.rename("exposure_scale").to_csv(exposure_path, index=True)
    else:
        exposure_path = None

    benchmark_candidates = [benchmark_code, "510300", "510500"]
    benchmark_codes = list(dict.fromkeys(benchmark_candidates))
    benchmark_df = compare_against_benchmarks(
        strategy_returns=result.daily_returns,
        benchmark_codes=benchmark_codes,
        source="baostock",
    )
    benchmark_csv_path, benchmark_md_path = save_benchmark_compare_outputs(
        benchmark_df,
        prefix="paper_rotation",
    )

    fills = simulate_paper_trades(result.weights, fee_bps=5.0, slippage_bps=5.0)
    fills_df = fills_to_frame(fills)
    fills_path = ROOT / "reports" / "paper_rotation_fills.csv"
    fills_df.to_csv(fills_path, index=False)

    report_text = render_markdown_report(
        metrics=result.metrics,
        latest_weights=result.weights.iloc[-1],
        start_date=pd.Timestamp(result.equity.index.min()),
        end_date=pd.Timestamp(result.equity.index.max()),
    )
    report_path = save_report(report_text, filename="paper_rotation_daily.md")

    qs_path = _try_generate_quantstats(result)
    baseline_stats = _try_run_backtestingpy_baseline(code=codes[0])

    baseline_text = ""
    if baseline_stats is not None:
        baseline_text = f"\n- Backtesting.py基准: {baseline_stats}"

    qs_text = ""
    if qs_path is not None:
        qs_text = f"\n- QuantStats报告: {qs_path}"

    quality_counts = quality_df["severity"].value_counts().to_dict() if not quality_df.empty else {}

    best_stab = stability_df.iloc[0].to_dict() if not stability_df.empty else {}
    best_benchmark = benchmark_df.iloc[0].to_dict() if not benchmark_df.empty else {}

    summary = (
        "纸盘运行完成\n"
        f"- 执行参数: {exec_params}\n"
        f"- 审批参数文件: {approved_path}\n"
        f"- 基准: {benchmark_code}\n"
        f"- 数据质量: {quality_counts}\n"
        f"- 数据质量CSV: {quality_csv_path}\n"
        f"- 数据质量MD: {quality_md_path}\n"
        f"- 指标: {result.metrics}\n"
        f"- 净值文件: {eq_path}\n"
        f"- 权重文件: {wt_path}\n"
        f"- 成交文件: {fills_path}\n"
        f"- 风险预算缩放: {exposure_path}\n"
        f"- 报告文件: {report_path}\n"
        f"- WalkForward窗口表: {wf_table_path}\n"
        f"- WalkForward净值: {wf_equity_path}\n"
        f"- 参数稳定性CSV: {stability_csv_path}\n"
        f"- 参数稳定性MD: {stability_md_path}\n"
        f"- 参数稳定性透视表数: {len(stability_pivot_csvs)}\n"
        f"- 参数稳定性热力图数: {len(stability_heatmaps)}\n"
        f"- 参数稳定性最佳组合: {best_stab}\n"
        f"- 研究建议JSON: {rec_json_path}\n"
        f"- 研究建议MD: {rec_md_path}\n"
        f"- 研究建议决策: {recommendation.get('decision')}\n"
        f"- 多基准对照CSV: {benchmark_csv_path}\n"
        f"- 多基准对照MD: {benchmark_md_path}\n"
        f"- 多基准最佳IR: {best_benchmark}"
        f"{qs_text}"
        f"{baseline_text}"
    )
    print(summary)
    push_dm(summary)


if __name__ == "__main__":
    main()
