import json
import os
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.backtest.vectorbt_runner import (
    load_amount_matrix,
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
from src.research import (
    RiskLimits,
    build_ai_research_review,
    build_regime_review,
    build_research_recommendation,
    evaluate_risk_guardrails,
    pick_regime_key_insight,
    save_ai_research_review,
    save_regime_review,
    save_research_recommendation,
    save_risk_guardrails_review,
)
from src.paper_trade import fills_to_frame, simulate_paper_trades
from src.reporting import render_markdown_report, save_report
from src.reporting.feishu_push import push_dm
from src.reporting.quantstats_report import generate_quantstats_html

DEFAULT_CODES = ["510300", "159915"]


def _discover_codes(source: str = "baostock", min_rows: int = 240):
    discovered = []

    # 兼容两种命名：
    # 1) etf_510300_baostock.csv
    # 2) etf_510300.csv
    source_files = list((ROOT / "data").glob(f"etf_*_{source}.csv"))
    generic_files = list((ROOT / "data").glob("etf_*.csv"))

    for f in source_files + generic_files:
        parts = f.stem.split("_")
        if len(parts) >= 2 and parts[1].isdigit() and len(parts[1]) == 6:
            try:
                row_n = len(pd.read_csv(f, usecols=["date"]))
            except Exception:
                row_n = 0
            if row_n >= min_rows:
                discovered.append(parts[1])

    discovered = sorted(set(discovered))
    if discovered:
        return discovered

    print(f"[warn] no dataset passed min_rows={min_rows}, fallback to default codes")
    return DEFAULT_CODES


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


def _load_config() -> dict:
    cfg_path = ROOT / "config.yaml"
    if not cfg_path.exists():
        return {}

    try:
        import yaml  # type: ignore

        data = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception as e:
        print(f"[warn] config.yaml parse failed: {e}")
        return {}


def main():
    codes = _discover_codes(source="baostock")
    benchmark_code = codes[0]
    cfg = _load_config()

    cfg_risk_limits = (((cfg.get("trading") or {}).get("risk_limits") or {}) if isinstance(cfg, dict) else {})
    cfg_cost_model = (((cfg.get("trading") or {}).get("cost_model") or {}) if isinstance(cfg, dict) else {})
    cfg_stop_guard = (((cfg.get("trading") or {}).get("stop_guard") or {}) if isinstance(cfg, dict) else {})
    cfg_quality = (((cfg.get("operations") or {}).get("quality_redline") or {}) if isinstance(cfg, dict) else {})
    cfg_regime = (((cfg.get("research") or {}).get("regime") or {}) if isinstance(cfg, dict) else {})
    cfg_ai_review = ((((cfg.get("research") or {}).get("ai_review") or {}).get("enabled")) if isinstance(cfg, dict) else True)

    approved_params, approved_path = _load_approved_params(prefix="paper_rotation")

    exec_params = {
        "top_n": int(approved_params.get("top_n", 2)),
        "min_score": float(approved_params.get("min_score", -0.1)),
        "vol_lookback": int(approved_params.get("vol_lookback", 20)),
        "max_turnover": float(approved_params.get("max_turnover", 0.8)),
        "target_vol_ann": float(approved_params.get("target_vol_ann", 0.12)),
        "drawdown_stop": float(approved_params.get("drawdown_stop", -0.05)),
        "dd_cooldown_days": int(approved_params.get("dd_cooldown_days", 5)),
        "fee_bps": float(approved_params.get("fee_bps", cfg_cost_model.get("fee_bps", 5.0))),
        "slippage_bps": float(approved_params.get("slippage_bps", cfg_cost_model.get("slippage_bps", 5.0))),
        "impact_bps": float(approved_params.get("impact_bps", cfg_cost_model.get("impact_bps", 2.0))),
        "impact_power": float(approved_params.get("impact_power", cfg_cost_model.get("impact_power", 0.5))),
        "impact_bps_cap_mult": float(approved_params.get("impact_bps_cap_mult", cfg_cost_model.get("impact_bps_cap_mult", 5.0))),
        "daily_loss_stop": float(approved_params.get("daily_loss_stop", cfg_stop_guard.get("daily_loss_stop", -0.03))),
        "monthly_drawdown_stop": float(approved_params.get("monthly_drawdown_stop", cfg_stop_guard.get("monthly_drawdown_stop", -0.10))),
        "stop_cooldown_days": int(approved_params.get("stop_cooldown_days", cfg_stop_guard.get("stop_cooldown_days", 3))),
    }

    quality_df = audit_universe(
        codes=codes,
        source="baostock",
        jump_threshold=0.12,
        min_rows_fail=int(cfg_quality.get("min_rows_fail", 240)),
        min_rows_warn=int(cfg_quality.get("min_rows_warn", 500)),
        severe_jump_threshold=float(cfg_quality.get("severe_jump_threshold", 0.25)),
        jump_warn_count=int(cfg_quality.get("jump_warn_count", 3)),
        missing_ratio_warn=float(cfg_quality.get("missing_ratio_warn", 0.01)),
    )
    quality_csv_path, quality_md_path = save_quality_reports(quality_df, prefix="paper_rotation")

    result = run_from_local_cache(
        codes=codes,
        source="baostock",
        rebalance="W-FRI",
        top_n=exec_params["top_n"],
        fee_bps=exec_params["fee_bps"],
        slippage_bps=exec_params["slippage_bps"],
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
        impact_bps=exec_params["impact_bps"],
        impact_power=exec_params["impact_power"],
        impact_bps_cap_mult=exec_params["impact_bps_cap_mult"],
        daily_loss_stop=exec_params["daily_loss_stop"],
        monthly_drawdown_stop=exec_params["monthly_drawdown_stop"],
        stop_cooldown_days=exec_params["stop_cooldown_days"],
    )

    risk_limits = RiskLimits(
        max_daily_drawdown=float(approved_params.get("max_daily_drawdown", cfg_risk_limits.get("max_daily_drawdown", -0.05))),
        max_total_drawdown=float(approved_params.get("max_total_drawdown", cfg_risk_limits.get("max_total_drawdown", -0.15))),
        max_position_weight=float(approved_params.get("max_position_weight", cfg_risk_limits.get("max_position_weight", 0.60))),
    )
    risk_review = evaluate_risk_guardrails(
        metrics=result.metrics,
        daily_returns=result.daily_returns,
        weights=result.weights,
        limits=risk_limits,
    )
    risk_json_path, risk_md_path = save_risk_guardrails_review(risk_review, prefix="paper_rotation")

    eq_path, wt_path = save_backtest_outputs(result, prefix="paper_rotation")

    wf_table, wf_returns = run_walk_forward_from_local_cache(
        codes=codes,
        source="baostock",
        rebalance="W-FRI",
        fee_bps=exec_params["fee_bps"],
        slippage_bps=exec_params["slippage_bps"],
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
        fee_bps=exec_params["fee_bps"],
        slippage_bps=exec_params["slippage_bps"],
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

    regime_df = build_regime_review(
        strategy_returns=result.daily_returns,
        benchmark_returns=result.benchmark_returns if result.benchmark_returns is not None else pd.Series(0.0, index=result.daily_returns.index),
        lookback_days=int(cfg_regime.get("lookback_days", 20)),
        bull_threshold=float(cfg_regime.get("bull_threshold", 0.03)),
        bear_threshold=float(cfg_regime.get("bear_threshold", -0.03)),
    )
    regime_csv_path, regime_md_path = save_regime_review(regime_df, prefix="paper_rotation")
    regime_insight = pick_regime_key_insight(regime_df)

    ai_json_path = None
    ai_md_path = None
    ai_enabled = bool(cfg_ai_review)
    if os.getenv("ENABLE_AI_RESEARCH_REVIEW", "").strip():
        ai_enabled = os.getenv("ENABLE_AI_RESEARCH_REVIEW", "1").strip().lower() not in {"0", "false", "no"}

    if ai_enabled:
        try:
            ai_context = {
                "metrics": result.metrics,
                "quality_counts": quality_df["severity"].value_counts().to_dict() if not quality_df.empty else {},
                "walk_forward": {
                    "avg_test_sharpe": float(wf_table["test_sharpe"].mean()) if "test_sharpe" in wf_table else 0.0,
                    "avg_test_annual_return": float(wf_table["test_annual_return"].mean()) if "test_annual_return" in wf_table else 0.0,
                    "worst_test_drawdown": float(wf_table["test_max_drawdown"].min()) if "test_max_drawdown" in wf_table else 0.0,
                },
                "stability_top": stability_df.head(3).to_dict(orient="records") if not stability_df.empty else [],
                "benchmark_top": benchmark_df.head(3).to_dict(orient="records") if not benchmark_df.empty else [],
                "regime_summary": regime_df.to_dict(orient="records") if not regime_df.empty else [],
                "recommendation": recommendation,
                "risk_guard": risk_review,
            }
            ai_review = build_ai_research_review(context=ai_context)
            ai_json_path, ai_md_path = save_ai_research_review(ai_review, prefix="paper_rotation")
        except Exception as e:
            print(f"[warn] AI研究报告生成失败: {e}")

    amount_df = load_amount_matrix(codes=codes, source="baostock").reindex(result.weights.index).fillna(0.0)
    fills = simulate_paper_trades(
        result.weights,
        fee_bps=exec_params["fee_bps"],
        slippage_bps=exec_params["slippage_bps"],
        amount_df=amount_df,
        impact_bps=exec_params["impact_bps"],
        impact_power=exec_params["impact_power"],
        impact_bps_cap_mult=exec_params["impact_bps_cap_mult"],
    )
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
    fill_cost_total = float(fills_df["est_cost"].sum()) if not fills_df.empty else 0.0
    fill_impact_total = float(fills_df["impact_cost"].sum()) if not fills_df.empty else 0.0

    summary = (
        f"纸盘运行完成（风险状态: {risk_review.get('status', 'PASS')}）\n"
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
        f"- 成本模型: fee={exec_params['fee_bps']}bps, slippage={exec_params['slippage_bps']}bps, impact={exec_params['impact_bps']}bps, power={exec_params['impact_power']}\n"
        f"- 停盘阈值: daily={exec_params['daily_loss_stop']}, monthly_dd={exec_params['monthly_drawdown_stop']}, cooldown={exec_params['stop_cooldown_days']}\n"
        f"- 回测总成本: {result.metrics.get('cost_total', 0.0):.4f}（冲击成本 {result.metrics.get('cost_impact', 0.0):.4f}）\n"
        f"- 停盘触发次数: daily={int(result.metrics.get('stop_trigger_daily', 0))}, monthly={int(result.metrics.get('stop_trigger_monthly', 0))}, total_dd={int(result.metrics.get('stop_trigger_total_dd', 0))}\n"
        f"- 模拟成交总成本: {fill_cost_total:.4f}（冲击成本 {fill_impact_total:.4f}）\n"
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
        f"- 风控检查JSON: {risk_json_path}\n"
        f"- 风控检查MD: {risk_md_path}\n"
        f"- 风控失败项: {risk_review.get('fail_items', [])}\n"
        f"- AI研究报告JSON: {ai_json_path}\n"
        f"- AI研究报告MD: {ai_md_path}\n"
        f"- 市场阶段复盘CSV: {regime_csv_path}\n"
        f"- 市场阶段复盘MD: {regime_md_path}\n"
        f"- 市场阶段关键洞察: {regime_insight}\n"
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
