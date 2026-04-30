import json
import os
import sys
from html import escape
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
TRADE_ALERT_STATE_PATH = ROOT / "reports" / "paper_rotation_trade_alert_state.json"

from src.backtest.vectorbt_runner import (
    load_amount_matrix,
    load_close_matrix,
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
    build_ai_referee_signals,
    evaluate_risk_guardrails,
    pick_regime_key_insight,
    save_ab_compare,
    save_ai_referee_outputs,
    save_ai_research_review,
    save_regime_review,
    save_research_recommendation,
    save_risk_guardrails_review,
)
from src.paper_trade import fills_to_frame, simulate_paper_trades
from src.reporting import render_markdown_report, save_report
from src.reporting.feishu_push import push_dm
from src.reporting.quantstats_report import generate_quantstats_html
from src.strategy import (
    build_backtest_template,
    build_signal_template,
    build_gatekeeper_metrics,
    classify_etf_frame,
    load_class_config,
    load_gatekeeper_config,
    score_gatekeeper,
)

DEFAULT_CODES = ["510300", "159915"]


def _load_trade_alert_state() -> dict:
    if not TRADE_ALERT_STATE_PATH.exists():
        return {}
    try:
        data = json.loads(TRADE_ALERT_STATE_PATH.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_trade_alert_state(data: dict) -> None:
    TRADE_ALERT_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    TRADE_ALERT_STATE_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


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
    max_codes_env = os.getenv("PAPER_ROTATION_MAX_CODES", "").strip()
    if max_codes_env:
        try:
            max_codes = max(1, int(max_codes_env))
            discovered = discovered[:max_codes]
        except Exception:
            pass

    if discovered:
        return discovered

    print(f"[warn] no dataset passed min_rows={min_rows}, fallback to default codes")
    return DEFAULT_CODES


def _pick_benchmark(codes):
    for c in ["510300", "159915", "510500"]:
        if c in set(codes):
            return c
    return codes[0]


def _save_backtest_dashboard_html(
    result,
    metrics: dict,
    window_snapshot: dict,
    validity: dict,
    exec_params: dict,
) -> Path:
    out = ROOT / "reports" / "paper_rotation_backtest_dashboard.html"
    out.parent.mkdir(parents=True, exist_ok=True)

    eq = result.equity.copy()
    if len(eq) > 500:
        eq = eq.iloc[:: max(1, len(eq) // 500)]
    if len(eq) < 2:
        polyline = ""
    else:
        e_min = float(eq.min())
        e_max = float(eq.max())
        span = max(1e-12, e_max - e_min)
        pts = []
        n = len(eq) - 1
        for i, v in enumerate(eq.tolist()):
            x = 20 + (760 * i / max(1, n))
            y = 240 - (180 * (float(v) - e_min) / span)
            pts.append(f"{x:.1f},{y:.1f}")
        polyline = " ".join(pts)

    html = f"""<!doctype html>
<html lang=\"zh-CN\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>策略回测看板</title>
  <style>
    body {{ font-family: 'Microsoft YaHei', 'PingFang SC', sans-serif; margin: 0; background: #f6f8fb; color: #223; }}
    .wrap {{ max-width: 980px; margin: 24px auto; padding: 0 16px; }}
    .card {{ background: #fff; border-radius: 12px; padding: 16px; margin-bottom: 14px; box-shadow: 0 2px 10px rgba(20,40,80,.06); }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 10px; }}
    .k {{ color: #667; font-size: 13px; }}
    .v {{ font-size: 20px; font-weight: 700; margin-top: 4px; }}
    .ok {{ color: #1f7a42; }} .warn {{ color: #b87312; }} .fail {{ color: #9d1b1b; }}
    a {{ color: #1460d2; text-decoration: none; }}
  </style>
</head>
<body>
  <div class=\"wrap\">
    <div class=\"card\">
      <h2 style=\"margin:0 0 8px;\">ETF策略回测看板</h2>
      <div class=\"k\">区间：{escape(str(window_snapshot.get('start')))} ~ {escape(str(window_snapshot.get('end')))} | 交易日 {int(window_snapshot.get('trading_days', 0))}</div>
    </div>

    <div class=\"card grid\">
      <div><div class=\"k\">累计收益</div><div class=\"v\">{float(metrics.get('total_return', 0.0)):+.2%}</div></div>
      <div><div class=\"k\">年化收益</div><div class=\"v\">{float(metrics.get('annual_return', 0.0)):+.2%}</div></div>
      <div><div class=\"k\">最大回撤</div><div class=\"v\">{float(metrics.get('max_drawdown', 0.0)):.2%}</div></div>
      <div><div class=\"k\">Sharpe</div><div class=\"v\">{float(metrics.get('sharpe', 0.0)):.2f}</div></div>
      <div><div class=\"k\">总成本(比例)</div><div class=\"v\">{float(metrics.get('cost_total', 0.0)):.4f}</div></div>
      <div><div class=\"k\">回测有效性评分</div><div class=\"v\">{int(validity.get('score', 0))}/100</div></div>
    </div>

    <div class=\"card\">
      <div class=\"k\" style=\"margin-bottom:8px;\">净值曲线（抽样）</div>
      <svg viewBox=\"0 0 800 260\" width=\"100%\" height=\"260\" style=\"background:#fcfdff;border-radius:8px;\">
        <polyline points=\"{polyline}\" fill=\"none\" stroke=\"#1460d2\" stroke-width=\"2\" />
      </svg>
    </div>

    <div class=\"card\">
      <div class=\"k\">执行与风控</div>
      <div>起始资金：{float(exec_params.get('init_cash', 0.0)):.2f} | 单日成交额占比上限：{float(exec_params.get('max_trade_amount_ratio', 0.0)):.2%}</div>
      <div>成交时间：{escape(str(exec_params.get('execution_time', '14:50')))} | 成本：fee={float(exec_params.get('fee_bps', 0.0))}bps, slippage={float(exec_params.get('slippage_bps', 0.0))}bps, impact={float(exec_params.get('impact_bps', 0.0))}bps</div>
    </div>

    <div class=\"card\">
      <div class=\"k\">本地文件</div>
      <div><a href=\"paper_rotation_daily.md\">日报</a> | <a href=\"paper_rotation_quantstats.html\">QuantStats</a> | <a href=\"paper_rotation_equity.csv\">净值CSV</a> | <a href=\"paper_rotation_walk_forward.csv\">WalkForward</a></div>
    </div>
  </div>
</body>
</html>
"""
    out.write_text(html, encoding="utf-8")
    return out


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


def _try_run_backtestingpy_baseline(code: str, cash: float):
    try:
        return run_backtestingpy_sma(code=code, source="baostock", fast=10, slow=30, commission=0.001, cash=float(cash))
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


def _clamp01(x: float) -> float:
    return max(0.0, min(1.0, float(x)))


def _extract_code6(v: object) -> str:
    s = str(v or "")
    digits = "".join(ch for ch in s if ch.isdigit())
    return digits[-6:] if len(digits) >= 6 else ""


def _load_universe_name_map() -> dict:
    path = ROOT / "reports" / "etf_market_snapshot_raw.csv"
    if not path.exists():
        return {}
    try:
        df = pd.read_csv(path)
    except Exception:
        return {}

    code_col = None
    for c in ["code", "代码", "基金代码", "symbol"]:
        if c in df.columns:
            code_col = c
            break

    name_col = None
    for c in ["name", "名称", "基金简称", "symbol_name"]:
        if c in df.columns:
            name_col = c
            break

    if code_col is None or name_col is None:
        return {}

    out = {}
    for _, r in df[[code_col, name_col]].dropna().iterrows():
        code6 = _extract_code6(r[code_col])
        if code6:
            out[code6] = str(r[name_col])
    return out


def _build_classification_snapshot(codes) -> tuple[pd.DataFrame, dict]:
    code_name_map = _load_universe_name_map()
    rows = [{"code": str(c), "name": code_name_map.get(str(c), str(c))} for c in list(codes)]
    base_df = pd.DataFrame(rows)
    cls_cfg = load_class_config()
    cls_df = classify_etf_frame(base_df, config=cls_cfg)

    class_counts = (
        cls_df["strategy_class"].value_counts().sort_values(ascending=False).to_dict()
        if not cls_df.empty and "strategy_class" in cls_df.columns
        else {}
    )
    dominant_class = next(iter(class_counts.keys()), "broad_index") if class_counts else "broad_index"
    dominant_backtest_template = "broad_index_backtest"
    if not cls_df.empty and "strategy_class" in cls_df.columns and "backtest_template" in cls_df.columns:
        hit = cls_df[cls_df["strategy_class"] == dominant_class]
        if not hit.empty:
            dominant_backtest_template = str(hit.iloc[0]["backtest_template"])

    snapshot = {
        "universe_size": int(len(cls_df)),
        "class_counts": {str(k): int(v) for k, v in class_counts.items()},
        "dominant_class": str(dominant_class),
        "dominant_backtest_template": str(dominant_backtest_template),
    }
    return cls_df, snapshot


def _build_gatekeeper_raw_metrics(benchmark_code: str, quality_df: pd.DataFrame, source: str = "baostock") -> dict:
    macro_risk = 0.5
    drawdown_risk = 0.5
    volatility_risk = 0.5

    try:
        bench_df = load_close_matrix(codes=[benchmark_code], source=source)
        bench = bench_df[benchmark_code].dropna() if benchmark_code in bench_df.columns else pd.Series(dtype=float)
        if len(bench) >= 30:
            ret = bench.pct_change(fill_method=None)
            ma120 = bench.rolling(120).mean()
            roll120 = bench.rolling(120).max()
            trend = float(bench.iloc[-1] / ma120.iloc[-1] - 1.0) if pd.notna(ma120.iloc[-1]) and ma120.iloc[-1] != 0 else 0.0
            vol20 = float(ret.rolling(20).std().iloc[-1]) if pd.notna(ret.rolling(20).std().iloc[-1]) else 0.02
            dd120 = float(bench.iloc[-1] / roll120.iloc[-1] - 1.0) if pd.notna(roll120.iloc[-1]) and roll120.iloc[-1] != 0 else -0.05

            if trend >= 0.05:
                trend_risk = 0.15
            elif trend >= 0.02:
                trend_risk = 0.30
            elif trend >= 0.00:
                trend_risk = 0.50
            elif trend >= -0.03:
                trend_risk = 0.70
            else:
                trend_risk = 0.90

            if vol20 <= 0.012:
                vol_risk = 0.20
            elif vol20 <= 0.018:
                vol_risk = 0.40
            elif vol20 <= 0.025:
                vol_risk = 0.65
            else:
                vol_risk = 0.85

            if dd120 >= -0.05:
                dd_risk = 0.25
            elif dd120 >= -0.10:
                dd_risk = 0.45
            elif dd120 >= -0.15:
                dd_risk = 0.70
            else:
                dd_risk = 0.90

            macro_risk = _clamp01(0.6 * trend_risk + 0.4 * vol_risk)
            volatility_risk = _clamp01(vol_risk)
            drawdown_risk = _clamp01(dd_risk)
    except Exception:
        pass

    breadth_risk = 0.50
    try:
        total = max(1, int(len(quality_df)))
        fail_n = int((quality_df["severity"] == "FAIL").sum()) if not quality_df.empty and "severity" in quality_df.columns else 0
        warn_n = int((quality_df["severity"] == "WARN").sum()) if not quality_df.empty and "severity" in quality_df.columns else 0
        fail_ratio = fail_n / total
        warn_ratio = warn_n / total
        breadth_risk = _clamp01(0.30 + fail_ratio * 1.4 + warn_ratio * 0.5)
    except Exception:
        pass

    return {
        "macro_risk": macro_risk,
        "drawdown_risk": drawdown_risk,
        "breadth_risk": breadth_risk,
        "volatility_risk": volatility_risk,
    }


def _build_macro_data_provenance(cfg: dict) -> dict:
    mb = (cfg.get("macro_bridge") or {}) if isinstance(cfg, dict) else {}
    source_path = str(mb.get("source_json", ""))
    source_name = Path(source_path).name if source_path else ""

    macro_ctx_path = ROOT / str(mb.get("output_json", "reports/macro_brain_context.json"))
    macro_feat_path = ROOT / "reports" / "macro_features.json"
    macro_hist_state_path = ROOT / "state" / "macro_history_3y_status.json"

    ctx_exists = macro_ctx_path.exists()
    feat_exists = macro_feat_path.exists()
    hist_state_exists = macro_hist_state_path.exists()

    hist_summary = {}
    if hist_state_exists:
        try:
            hist_payload = json.loads(macro_hist_state_path.read_text(encoding="utf-8"))
            hist_summary = (hist_payload.get("summary") or {}) if isinstance(hist_payload, dict) else {}
        except Exception:
            hist_summary = {}

    source_mode = "unknown"
    has_historical_news_series = False
    if source_name == "ai_context_pack.json":
        source_mode = "point_in_time_news_pack"
        has_historical_news_series = False
    elif source_name:
        source_mode = "custom_json"

    return {
        "macro_bridge_enabled": bool(mb.get("enabled", False)),
        "source_json": source_path,
        "source_mode": source_mode,
        "has_historical_news_series": has_historical_news_series,
        "macro_context_exists": ctx_exists,
        "macro_features_exists": feat_exists,
        "macro_history_3y_state_exists": hist_state_exists,
        "macro_history_3y_summary": hist_summary,
        "backtest_gatekeeper_driver": "price_quality_proxy",
        "notes": [
            "Current gatekeeper macro_risk is computed from benchmark trend/volatility plus quality breadth.",
            "Historical macro history exists for FRED series, but it is not currently wired into gatekeeper time-series state transitions.",
        ],
    }


def _build_macro_history_sentiment_proxy(trading_index: pd.Index, macro_provenance: dict) -> tuple[pd.Series, dict]:
    empty = pd.Series(0.0, index=trading_index, dtype=float)
    summary = {
        "enabled": False,
        "driver": "price_quality_proxy",
        "coverage_ratio": 0.0,
        "coverage_days": 0,
        "total_days": int(len(trading_index)),
        "state_counts": {"green": 0, "yellow": 0, "red": 0},
        "no_lookahead_shift_days": 1,
    }

    hist = (macro_provenance.get("macro_history_3y_summary") or {}) if isinstance(macro_provenance, dict) else {}
    merged_path = str(hist.get("merged_path") or "")
    if not merged_path:
        return empty, summary

    path = Path(merged_path)
    if not path.exists():
        return empty, summary

    try:
        df = pd.read_csv(path)
        if "date" not in df.columns:
            return empty, summary
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date"]).sort_values("date").set_index("date")

        for col in ["DGS10", "DGS2", "DTWEXBGS", "DCOILWTICO", "T10YIE"]:
            if col not in df.columns:
                df[col] = pd.NA
            df[col] = pd.to_numeric(df[col], errors="coerce")

        spread = (df["DGS10"] - df["DGS2"]).ffill()
        usd_60 = df["DTWEXBGS"].ffill().pct_change(60, fill_method=None).fillna(0.0)
        oil_20 = df["DCOILWTICO"].ffill().pct_change(20, fill_method=None).fillna(0.0)
        be10 = df["T10YIE"].ffill()

        risk = pd.Series(0.35, index=df.index, dtype=float)
        risk = risk + (spread < 0.0).astype(float) * 0.25
        risk = risk + (usd_60 > 0.03).astype(float) * 0.20
        risk = risk + (oil_20 > 0.12).astype(float) * 0.15
        risk = risk + (be10 > 2.8).astype(float) * 0.10
        risk = risk.clip(lower=0.0, upper=1.0)

        sentiment = ((0.5 - risk) / 0.5).clip(lower=-1.0, upper=1.0)
        sentiment = sentiment.rolling(5, min_periods=1).mean()
        sentiment = sentiment.shift(1)
        sentiment = sentiment.reindex(pd.to_datetime(trading_index)).ffill().fillna(0.0)
        sentiment.index = trading_index

        state = pd.Series("yellow", index=sentiment.index, dtype=object)
        state.loc[sentiment >= 0.2] = "green"
        state.loc[sentiment <= -0.3] = "red"
        state_counts = state.value_counts().to_dict()

        coverage_days = int(sentiment.notna().sum())
        total_days = int(len(sentiment))
        coverage_ratio = float(coverage_days / total_days) if total_days > 0 else 0.0

        summary = {
            "enabled": True,
            "driver": "macro_history_series_proxy",
            "coverage_ratio": round(coverage_ratio, 4),
            "coverage_days": coverage_days,
            "total_days": total_days,
            "state_counts": {
                "green": int(state_counts.get("green", 0)),
                "yellow": int(state_counts.get("yellow", 0)),
                "red": int(state_counts.get("red", 0)),
            },
            "no_lookahead_shift_days": 1,
        }
        return sentiment, summary
    except Exception:
        return empty, summary


def _apply_gatekeeper_to_exec_params(exec_params: dict, gate_result) -> dict:
    out = dict(exec_params)
    actions = gate_result.actions if gate_result is not None else {}
    gross_mult = float(actions.get("gross_exposure_mult", 1.0))
    turnover_mult = float(actions.get("turnover_mult", 1.0))
    allow_new_entries = bool(actions.get("allow_new_entries", True))

    out["target_vol_ann"] = max(0.01, float(out["target_vol_ann"]) * gross_mult)
    out["max_turnover"] = max(0.05, min(1.0, float(out["max_turnover"]) * turnover_mult))

    state = str(getattr(gate_result, "state", "green"))
    if state == "yellow":
        out["target_vol_ann"] = min(float(out["target_vol_ann"]), 0.05)
        out["max_turnover"] = min(float(out["max_turnover"]), 0.35)
        out["drawdown_stop"] = max(float(out["drawdown_stop"]), -0.05)
        out["dd_cooldown_days"] = max(int(out["dd_cooldown_days"]), 5)
        out["dd_rearm_days"] = max(int(out["dd_rearm_days"]), 60)
        out["monthly_drawdown_stop"] = max(float(out["monthly_drawdown_stop"]), -0.07)
        out["stop_cooldown_days"] = max(int(out["stop_cooldown_days"]), 7)
        out["reentry_cooldown_periods"] = max(int(out["reentry_cooldown_periods"]), 2)
        out["entry_confirm_periods"] = max(int(out["entry_confirm_periods"]), 3)
        out["max_trade_amount_ratio"] = max(0.005, min(float(out["max_trade_amount_ratio"]) * 0.6, 0.02))
    elif state == "red":
        out["target_vol_ann"] = min(float(out["target_vol_ann"]), 0.04)
        out["max_turnover"] = min(float(out["max_turnover"]), 0.25)
        out["monthly_drawdown_stop"] = max(float(out["monthly_drawdown_stop"]), -0.06)
        out["daily_loss_stop"] = max(float(out["daily_loss_stop"]), -0.02)
        out["stop_cooldown_days"] = max(int(out["stop_cooldown_days"]), 10)
        out["reentry_cooldown_periods"] = max(int(out["reentry_cooldown_periods"]), 3)
        out["entry_confirm_periods"] = max(int(out["entry_confirm_periods"]), 3)
        out["max_trade_amount_ratio"] = max(0.003, min(float(out["max_trade_amount_ratio"]) * 0.4, 0.015))

    if not allow_new_entries:
        out["min_score"] = max(float(out["min_score"]), 1.0)
        out["buy_threshold"] = max(float(out["buy_threshold"]), 1.0)

    out["gatekeeper_state"] = str(getattr(gate_result, "state", "green"))
    out["gatekeeper_score"] = float(getattr(gate_result, "score", 0.0))
    out["gatekeeper_actions"] = actions
    return out


def _apply_class_template_to_exec_params(exec_params: dict, cls_snapshot: dict) -> dict:
    out = dict(exec_params)
    template_name = str(cls_snapshot.get("dominant_backtest_template", "broad_index_backtest"))
    template = build_backtest_template(template_name)

    out["fee_bps"] = max(float(out["fee_bps"]), float(template.get("fee_bps", out["fee_bps"])))
    out["slippage_bps"] = max(float(out["slippage_bps"]), float(template.get("slippage_bps", out["slippage_bps"])))
    out["top_n"] = max(1, min(int(out["top_n"]), int(template.get("holding_limit", out["top_n"]))))
    out["dominant_strategy_class"] = str(cls_snapshot.get("dominant_class", "broad_index"))
    out["dominant_backtest_template"] = template_name
    return out


def _derive_tradable_codes(codes: list[str], quality_df: pd.DataFrame, exclude_fail: bool, prefix: str = "paper_rotation"):
    if (not exclude_fail) or quality_df is None or quality_df.empty or "severity" not in quality_df.columns:
        return list(codes), [], None

    code_col = None
    for c in ["code", "代码", "symbol"]:
        if c in quality_df.columns:
            code_col = c
            break
    if code_col is None:
        return list(codes), [], None

    fail_df = quality_df[quality_df["severity"].astype(str).str.upper() == "FAIL"].copy()
    fail_codes = []
    for v in fail_df[code_col].tolist():
        code6 = _extract_code6(v)
        if code6:
            fail_codes.append(code6)
    fail_set = set(fail_codes)

    tradable = [str(c) for c in codes if str(c) not in fail_set]
    excluded = [str(c) for c in codes if str(c) in fail_set]

    out_path = ROOT / "reports" / f"{prefix}_excluded_fail_codes.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"code": excluded, "reason": "quality_fail"}).to_csv(out_path, index=False, encoding="utf-8-sig")

    return tradable, excluded, out_path


def _build_asset_params_from_classification(cls_df: pd.DataFrame, exec_params: dict) -> dict:
    if cls_df is None or cls_df.empty:
        return {}

    out = {}
    for _, row in cls_df.iterrows():
        code6 = _extract_code6(row.get("code"))
        if not code6:
            continue

        strategy_template = str(row.get("strategy_template", "momentum_core"))
        sig_tpl = build_signal_template(strategy_template)
        lookbacks = [int(x) for x in (sig_tpl.get("lookbacks") or []) if str(x).isdigit()]
        if not lookbacks:
            lookbacks = [int(exec_params.get("score_mom_short", 20)), int(exec_params.get("score_mom_long", 60))]

        mom_short = max(1, min(lookbacks))
        mom_long = max(1, max(lookbacks))
        risk_overlay = bool(sig_tpl.get("risk_overlay", False))

        if strategy_template == "momentum_plus_risk":
            w_mom_short, w_mom_long, w_low_vol, w_low_drawdown = 0.40, 0.30, 0.15, 0.15
        elif strategy_template == "carry_defensive":
            w_mom_short, w_mom_long, w_low_vol, w_low_drawdown = 0.20, 0.20, 0.30, 0.30
        elif strategy_template == "macro_sensitive":
            w_mom_short, w_mom_long, w_low_vol, w_low_drawdown = 0.35, 0.25, 0.20, 0.20
        elif strategy_template == "global_beta":
            w_mom_short, w_mom_long, w_low_vol, w_low_drawdown = 0.35, 0.35, 0.15, 0.15
        else:
            w_mom_short = float(exec_params.get("score_w_mom_short", 0.50))
            w_mom_long = float(exec_params.get("score_w_mom_long", 0.30))
            w_low_vol = float(exec_params.get("score_w_low_vol", 0.10))
            w_low_drawdown = float(exec_params.get("score_w_low_drawdown", 0.10))

        out[code6] = {
            "strategy_class": str(row.get("strategy_class", "broad_index")),
            "strategy_template": strategy_template,
            "mom_short": mom_short,
            "mom_long": mom_long,
            "vol_window": int(exec_params.get("score_vol_window", 20)),
            "dd_window": int(exec_params.get("score_dd_window", 60)),
            "w_mom_short": float(w_mom_short),
            "w_mom_long": float(w_mom_long),
            "w_low_vol": float(w_low_vol if risk_overlay else max(0.05, w_low_vol)),
            "w_low_drawdown": float(w_low_drawdown if risk_overlay else max(0.05, w_low_drawdown)),
        }

    return out


def _build_gatekeeper_class_constraints(exec_params: dict, asset_params: dict) -> dict:
    gate_state = str(exec_params.get("gatekeeper_state", "green")).lower()
    if gate_state == "red":
        return {
            "allowed_classes": ["broad_index", "bond", "commodity_gold"],
            "class_max_positions": {
                "broad_index": 1,
                "bond": 1,
                "commodity_gold": 1,
                "sector_theme": 0,
                "cross_border": 0,
            },
            "class_exposure_caps": {
                "broad_index": 0.50,
                "bond": 0.50,
                "commodity_gold": 0.30,
                "sector_theme": 0.00,
                "cross_border": 0.00,
            },
        }
    if gate_state == "yellow":
        return {
            "allowed_classes": None,
            "class_max_positions": {
                "sector_theme": 1,
                "cross_border": 1,
                "bond": 2,
                "commodity_gold": 2,
            },
            "class_exposure_caps": {
                "sector_theme": 0.15,
                "cross_border": 0.08,
                "bond": 0.45,
                "commodity_gold": 0.25,
            },
        }
    return {"allowed_classes": None, "class_max_positions": {}, "class_exposure_caps": {}}


def _save_strategy_effective_snapshot(
    exec_params: dict,
    class_snapshot: dict,
    asset_params: dict,
    gate_result,
    class_constraints: dict,
) -> Path:
    out = ROOT / "reports" / "paper_rotation_strategy_effective_snapshot.json"
    template_counts = {}
    class_counts = {}
    for cfg in asset_params.values():
        tpl = str(cfg.get("strategy_template", "unknown"))
        klass = str(cfg.get("strategy_class", "unknown"))
        template_counts[tpl] = int(template_counts.get(tpl, 0) + 1)
        class_counts[klass] = int(class_counts.get(klass, 0) + 1)

    payload = {
        "dominant_strategy_class": exec_params.get("dominant_strategy_class"),
        "dominant_backtest_template": exec_params.get("dominant_backtest_template"),
        "gatekeeper": gate_result.to_dict() if gate_result is not None else {},
        "effective_exec_params": {
            "top_n": exec_params.get("top_n"),
            "min_score": exec_params.get("min_score"),
            "buy_threshold": exec_params.get("buy_threshold"),
            "sell_threshold": exec_params.get("sell_threshold"),
            "target_vol_ann": exec_params.get("target_vol_ann"),
            "max_turnover": exec_params.get("max_turnover"),
            "fee_bps": exec_params.get("fee_bps"),
            "slippage_bps": exec_params.get("slippage_bps"),
        },
        "classification_snapshot": class_snapshot,
        "asset_strategy_templates": {
            "count": int(len(asset_params)),
            "template_counts": template_counts,
            "class_counts": class_counts,
            "samples": list(asset_params.items())[:12],
        },
        "class_constraints": class_constraints,
    }
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return out


def _build_window_snapshot(result, benchmark_code: str, source: str = "baostock") -> dict:
    if result.equity.empty:
        return {
            "start": None,
            "end": None,
            "trading_days": 0,
            "calendar_days": 0,
            "regime_days": {},
        }

    start = pd.Timestamp(result.equity.index.min())
    end = pd.Timestamp(result.equity.index.max())
    trading_days = int(len(result.equity))
    calendar_days = int((end - start).days) + 1

    regime_days = {"bull": 0, "bear": 0, "sideways": 0}
    try:
        bench = load_close_matrix(codes=[benchmark_code], source=source)[benchmark_code].reindex(result.equity.index).ffill()
        r20 = bench.pct_change(20)
        for v in r20.fillna(0.0).tolist():
            x = float(v)
            if x >= 0.03:
                regime_days["bull"] += 1
            elif x <= -0.03:
                regime_days["bear"] += 1
            else:
                regime_days["sideways"] += 1
    except Exception:
        regime_days = {"bull": 0, "bear": 0, "sideways": trading_days}

    return {
        "start": str(start.date()),
        "end": str(end.date()),
        "trading_days": trading_days,
        "calendar_days": calendar_days,
        "regime_days": regime_days,
    }


def _backtest_validity_score(window_snapshot: dict, wf_table: pd.DataFrame, quality_df: pd.DataFrame, cost_total: float) -> dict:
    score = 0
    checks = {}

    td = int(window_snapshot.get("trading_days", 0))
    if td >= 500:
        score += 35
        checks["window"] = "PASS"
    elif td >= 250:
        score += 22
        checks["window"] = "WARN"
    else:
        score += 10
        checks["window"] = "FAIL"

    wf_n = int(len(wf_table)) if wf_table is not None else 0
    if wf_n >= 8:
        score += 25
        checks["walk_forward"] = "PASS"
    elif wf_n >= 4:
        score += 15
        checks["walk_forward"] = "WARN"
    else:
        score += 5
        checks["walk_forward"] = "FAIL"

    fail_n = int((quality_df["severity"] == "FAIL").sum()) if not quality_df.empty and "severity" in quality_df.columns else 0
    if fail_n <= 3:
        score += 20
        checks["data_quality"] = "PASS"
    elif fail_n <= 8:
        score += 12
        checks["data_quality"] = "WARN"
    else:
        score += 4
        checks["data_quality"] = "FAIL"

    if float(cost_total) > 0:
        score += 20
        checks["cost_model"] = "PASS"
    else:
        score += 0
        checks["cost_model"] = "FAIL"

    return {
        "score": int(max(0, min(100, score))),
        "checks": checks,
        "wf_windows": wf_n,
        "quality_fail_count": fail_n,
    }


def _build_macro_alignment_audit(macro_provenance: dict, window_snapshot: dict) -> dict:
    start = str(window_snapshot.get("start") or "")
    end = str(window_snapshot.get("end") or "")
    hist = (macro_provenance.get("macro_history_3y_summary") or {}) if isinstance(macro_provenance, dict) else {}
    hist_start = str(hist.get("merged_min_date") or "")
    hist_end = str(hist.get("merged_max_date") or "")

    coverage = "unknown"
    if start and end and hist_start and hist_end:
        if hist_start <= start and hist_end >= end:
            coverage = "full"
        elif hist_end < start or hist_start > end:
            coverage = "none"
        else:
            coverage = "partial"

    has_hist_news = bool(macro_provenance.get("has_historical_news_series", False))
    driver = str(macro_provenance.get("backtest_gatekeeper_driver", "unknown"))
    source_mode = str(macro_provenance.get("source_mode", "unknown"))

    if has_hist_news and driver == "macro_news_series":
        realism = "aligned"
    elif driver == "macro_history_series_proxy":
        realism = "partial"
    elif source_mode == "point_in_time_news_pack" and driver != "macro_news_series":
        realism = "mismatch"
    else:
        realism = "partial"

    return {
        "backtest_window": {"start": start, "end": end},
        "macro_history_window": {"start": hist_start, "end": hist_end, "coverage": coverage},
        "source_mode": source_mode,
        "has_historical_news_series": has_hist_news,
        "gatekeeper_driver": driver,
        "realism_status": realism,
        "no_lookahead_assumption": "only_past_data_if_time_series_wired",
    }


def _sentiment_exposure_cap_by_gate(exec_params: dict, base_cap: float) -> float:
    state = str(exec_params.get("gatekeeper_state", "green")).lower()
    cap = float(base_cap)
    if state == "yellow":
        cap = min(cap, 0.22)
    elif state == "red":
        cap = min(cap, 0.12)
    else:
        cap = min(cap, 0.35)
    return max(0.0, cap)


def main():
    cfg = _load_config()
    macro_provenance = _build_macro_data_provenance(cfg)
    macro_provenance_path = ROOT / "reports" / "paper_rotation_macro_data_provenance.json"
    macro_provenance_path.write_text(json.dumps(macro_provenance, ensure_ascii=False, indent=2), encoding="utf-8")
    cfg_backtest = (((cfg.get("operations") or {}).get("backtest_validation") or {}) if isinstance(cfg, dict) else {})
    trading_days_per_year = int(cfg_backtest.get("trading_days_per_year", 240))
    min_backtest_years = float(cfg_backtest.get("min_years", 2.0))
    discover_min_rows = max(240, int(trading_days_per_year * min_backtest_years))

    codes = _discover_codes(source="baostock", min_rows=discover_min_rows)
    benchmark_code = _pick_benchmark(codes)

    cfg_risk_limits = (((cfg.get("trading") or {}).get("risk_limits") or {}) if isinstance(cfg, dict) else {})
    cfg_cost_model = (((cfg.get("trading") or {}).get("cost_model") or {}) if isinstance(cfg, dict) else {})
    cfg_stop_guard = (((cfg.get("trading") or {}).get("stop_guard") or {}) if isinstance(cfg, dict) else {})
    cfg_regime_filter = (((cfg.get("trading") or {}).get("regime_filter") or {}) if isinstance(cfg, dict) else {})
    cfg_strategy_score = (((cfg.get("trading") or {}).get("strategy_score") or {}) if isinstance(cfg, dict) else {})
    cfg_timing_switch = (((cfg.get("trading") or {}).get("timing_switch") or {}) if isinstance(cfg, dict) else {})
    cfg_adaptive_top_n = (((cfg.get("trading") or {}).get("adaptive_top_n") or {}) if isinstance(cfg, dict) else {})
    cfg_anti_whipsaw = (((cfg.get("trading") or {}).get("anti_whipsaw") or {}) if isinstance(cfg, dict) else {})
    cfg_ai_referee = (((cfg.get("trading") or {}).get("ai_referee") or {}) if isinstance(cfg, dict) else {})
    cfg_quality = (((cfg.get("operations") or {}).get("quality_redline") or {}) if isinstance(cfg, dict) else {})
    cfg_targets = (((cfg.get("operations") or {}).get("simulation_targets") or {}) if isinstance(cfg, dict) else {})
    cfg_notify = (((cfg.get("operations") or {}).get("notify") or {}) if isinstance(cfg, dict) else {})
    cfg_regime = (((cfg.get("research") or {}).get("regime") or {}) if isinstance(cfg, dict) else {})
    cfg_ai_review = ((((cfg.get("research") or {}).get("ai_review") or {}).get("enabled")) if isinstance(cfg, dict) else True)
    cfg_trading = ((cfg.get("trading") or {}) if isinstance(cfg, dict) else {})

    approved_params, approved_path = _load_approved_params(prefix="paper_rotation")

    exec_params = {
        "top_n": int(approved_params.get("top_n", 2)),
        "min_score": float(approved_params.get("min_score", -0.1)),
        "vol_lookback": int(approved_params.get("vol_lookback", 20)),
        "max_turnover": float(approved_params.get("max_turnover", 0.8)),
        "target_vol_ann": float(approved_params.get("target_vol_ann", 0.12)),
        "drawdown_stop": float(approved_params.get("drawdown_stop", -0.05)),
        "drawdown_recovery": float(
            approved_params.get(
                "drawdown_recovery",
                max(float(approved_params.get("drawdown_stop", -0.05)) * 0.5, -0.02),
            )
        ),
        "dd_cooldown_days": int(approved_params.get("dd_cooldown_days", 5)),
        "dd_rearm_days": int(approved_params.get("dd_rearm_days", 60)),
        "fee_bps": float(approved_params.get("fee_bps", cfg_cost_model.get("fee_bps", 5.0))),
        "slippage_bps": float(approved_params.get("slippage_bps", cfg_cost_model.get("slippage_bps", 5.0))),
        "impact_bps": float(approved_params.get("impact_bps", cfg_cost_model.get("impact_bps", 2.0))),
        "impact_power": float(approved_params.get("impact_power", cfg_cost_model.get("impact_power", 0.5))),
        "impact_bps_cap_mult": float(approved_params.get("impact_bps_cap_mult", cfg_cost_model.get("impact_bps_cap_mult", 5.0))),
        "daily_loss_stop": float(approved_params.get("daily_loss_stop", cfg_stop_guard.get("daily_loss_stop", -0.03))),
        "monthly_drawdown_stop": float(approved_params.get("monthly_drawdown_stop", cfg_stop_guard.get("monthly_drawdown_stop", -0.10))),
        "stop_cooldown_days": int(approved_params.get("stop_cooldown_days", cfg_stop_guard.get("stop_cooldown_days", 3))),
        "regime_filter_enabled": bool(approved_params.get("regime_filter_enabled", cfg_regime_filter.get("enabled", True))),
        "regime_ma_window": int(approved_params.get("regime_ma_window", cfg_regime_filter.get("ma_window", 200))),
        "regime_vol_window": int(approved_params.get("regime_vol_window", cfg_regime_filter.get("vol_window", 20))),
        "regime_high_vol_threshold": float(approved_params.get("regime_high_vol_threshold", cfg_regime_filter.get("high_vol_threshold", 0.02))),
        "regime_defensive_exposure": float(approved_params.get("regime_defensive_exposure", cfg_regime_filter.get("defensive_exposure", 0.30))),
        "score_mom_short": int(approved_params.get("score_mom_short", cfg_strategy_score.get("mom_short", 20))),
        "score_mom_long": int(approved_params.get("score_mom_long", cfg_strategy_score.get("mom_long", 60))),
        "score_vol_window": int(approved_params.get("score_vol_window", cfg_strategy_score.get("vol_window", 20))),
        "score_dd_window": int(approved_params.get("score_dd_window", cfg_strategy_score.get("dd_window", 60))),
        "score_w_mom_short": float(approved_params.get("score_w_mom_short", cfg_strategy_score.get("w_mom_short", 0.50))),
        "score_w_mom_long": float(approved_params.get("score_w_mom_long", cfg_strategy_score.get("w_mom_long", 0.30))),
        "score_w_low_vol": float(approved_params.get("score_w_low_vol", cfg_strategy_score.get("w_low_vol", 0.10))),
        "score_w_low_drawdown": float(approved_params.get("score_w_low_drawdown", cfg_strategy_score.get("w_low_drawdown", 0.10))),
        "timing_switch_enabled": bool(approved_params.get("timing_switch_enabled", cfg_timing_switch.get("enabled", True))),
        "trend_short_ma": int(approved_params.get("trend_short_ma", cfg_timing_switch.get("short_ma", 20))),
        "trend_long_ma": int(approved_params.get("trend_long_ma", cfg_timing_switch.get("long_ma", 120))),
        "trend_gate_threshold": float(approved_params.get("trend_gate_threshold", cfg_timing_switch.get("gate_threshold", 0.0))),
        "trend_amplify_threshold": float(approved_params.get("trend_amplify_threshold", cfg_timing_switch.get("amplify_threshold", 0.02))),
        "trend_amplify_mult": float(approved_params.get("trend_amplify_mult", cfg_timing_switch.get("amplify_mult", 1.20))),
        "trend_defensive_scale": float(approved_params.get("trend_defensive_scale", cfg_timing_switch.get("defensive_scale", 0.50))),
        "adaptive_top_n_enabled": bool(approved_params.get("adaptive_top_n_enabled", cfg_adaptive_top_n.get("enabled", True))),
        "top_n_strong": int(approved_params.get("top_n_strong", cfg_adaptive_top_n.get("top_n_strong", 1))),
        "top_n_neutral": int(approved_params.get("top_n_neutral", cfg_adaptive_top_n.get("top_n_neutral", 2))),
        "top_n_weak": int(approved_params.get("top_n_weak", cfg_adaptive_top_n.get("top_n_weak", 4))),
        "trend_strong_threshold": float(approved_params.get("trend_strong_threshold", cfg_adaptive_top_n.get("trend_strong_threshold", 0.03))),
        "trend_weak_threshold": float(approved_params.get("trend_weak_threshold", cfg_adaptive_top_n.get("trend_weak_threshold", 0.0))),
        "buy_threshold": float(approved_params.get("buy_threshold", cfg_anti_whipsaw.get("buy_threshold", -0.05))),
        "sell_threshold": float(approved_params.get("sell_threshold", cfg_anti_whipsaw.get("sell_threshold", -0.12))),
        "entry_confirm_periods": int(approved_params.get("entry_confirm_periods", cfg_anti_whipsaw.get("entry_confirm_periods", 2))),
        "min_hold_rebalance_periods": int(approved_params.get("min_hold_rebalance_periods", cfg_anti_whipsaw.get("min_hold_rebalance_periods", 2))),
        "reentry_cooldown_periods": int(approved_params.get("reentry_cooldown_periods", cfg_anti_whipsaw.get("reentry_cooldown_periods", 1))),
        "ai_referee_enabled": bool(approved_params.get("ai_referee_enabled", cfg_ai_referee.get("enabled", True))),
        "ai_referee_exposure_cap": float(approved_params.get("ai_referee_exposure_cap", cfg_ai_referee.get("exposure_cap", 0.15))),
        "ai_referee_max_points": int(approved_params.get("ai_referee_max_points", cfg_ai_referee.get("max_llm_points", 60))),
        "ai_referee_apply_if_better": bool(approved_params.get("ai_referee_apply_if_better", cfg_ai_referee.get("apply_if_better", True))),
        "init_cash": float(approved_params.get("init_cash", cfg_trading.get("init_cash", 10000.0))),
        "execution_time": str(approved_params.get("execution_time", cfg_trading.get("execution_time", "14:50"))),
        "max_trade_amount_ratio": float(approved_params.get("max_trade_amount_ratio", cfg_cost_model.get("max_trade_amount_ratio", 0.05))),
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

    exclude_fail_from_backtest = bool(cfg_quality.get("exclude_fail_from_backtest", True))
    tradable_codes, excluded_fail_codes, excluded_fail_path = _derive_tradable_codes(
        codes=list(codes),
        quality_df=quality_df,
        exclude_fail=exclude_fail_from_backtest,
        prefix="paper_rotation",
    )
    if len(tradable_codes) < max(5, min(20, len(codes) // 4)):
        print("[warn] tradable codes too few after quality FAIL exclusion, fallback to original discovered codes")
        tradable_codes = list(codes)
        excluded_fail_codes = []
        excluded_fail_path = None

    benchmark_code = _pick_benchmark(tradable_codes if tradable_codes else codes)

    class_df, class_snapshot = _build_classification_snapshot(tradable_codes)
    class_snapshot_path = ROOT / "reports" / "paper_rotation_strategy_class_snapshot.json"
    class_snapshot_path.write_text(json.dumps(class_snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
    class_csv_path = ROOT / "reports" / "paper_rotation_strategy_classification.csv"
    class_df.to_csv(class_csv_path, index=False, encoding="utf-8-sig")

    gate_cfg = load_gatekeeper_config()
    gate_raw_metrics = _build_gatekeeper_raw_metrics(
        benchmark_code=benchmark_code,
        quality_df=quality_df,
        source="baostock",
    )
    gate_metrics = build_gatekeeper_metrics(gate_raw_metrics)
    gate_result = score_gatekeeper(gate_metrics, config=gate_cfg)
    gate_snapshot_path = ROOT / "reports" / "paper_rotation_gatekeeper.json"
    gate_snapshot_path.write_text(json.dumps(gate_result.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")

    exec_params = _apply_class_template_to_exec_params(exec_params, class_snapshot)
    exec_params = _apply_gatekeeper_to_exec_params(exec_params, gate_result)
    asset_params = _build_asset_params_from_classification(cls_df=class_df, exec_params=exec_params)
    class_constraints = _build_gatekeeper_class_constraints(exec_params=exec_params, asset_params=asset_params)
    asset_class_map = {str(k): str(v.get("strategy_class", "unknown")) for k, v in asset_params.items()}
    sentiment_exposure_cap = _sentiment_exposure_cap_by_gate(
        exec_params=exec_params,
        base_cap=float(exec_params.get("ai_referee_exposure_cap", 0.15)),
    )

    bench_index = load_close_matrix(codes=[benchmark_code], source="baostock").index
    macro_sentiment, macro_sent_summary = _build_macro_history_sentiment_proxy(
        trading_index=bench_index,
        macro_provenance=macro_provenance,
    )
    macro_regime_path = ROOT / "reports" / "paper_rotation_macro_regime_proxy.csv"
    pd.DataFrame(
        {
            "date": macro_sentiment.index,
            "macro_sentiment": macro_sentiment.values,
        }
    ).to_csv(macro_regime_path, index=False, encoding="utf-8")

    macro_provenance["backtest_gatekeeper_driver"] = str(macro_sent_summary.get("driver", "price_quality_proxy"))
    macro_provenance["macro_sentiment_proxy"] = macro_sent_summary
    macro_provenance_path.write_text(json.dumps(macro_provenance, ensure_ascii=False, indent=2), encoding="utf-8")

    strategy_effective_snapshot_path = _save_strategy_effective_snapshot(
        exec_params=exec_params,
        class_snapshot=class_snapshot,
        asset_params=asset_params,
        gate_result=gate_result,
        class_constraints=class_constraints,
    )

    baseline_result = run_from_local_cache(
        codes=tradable_codes,
        source="baostock",
        rebalance="W-FRI",
        top_n=exec_params["top_n"],
        fee_bps=exec_params["fee_bps"],
        slippage_bps=exec_params["slippage_bps"],
        min_score=exec_params["min_score"],
        sentiment=macro_sentiment,
        benchmark_code=benchmark_code,
        max_turnover=exec_params["max_turnover"],
        use_risk_parity=True,
        vol_lookback=exec_params["vol_lookback"],
        target_vol_ann=exec_params["target_vol_ann"],
        vol_target_lookback=20,
        max_leverage=1.0,
        drawdown_stop=exec_params["drawdown_stop"],
        drawdown_recovery=exec_params["drawdown_recovery"],
        dd_cooldown_days=exec_params["dd_cooldown_days"],
        dd_rearm_days=exec_params["dd_rearm_days"],
        impact_bps=exec_params["impact_bps"],
        impact_power=exec_params["impact_power"],
        impact_bps_cap_mult=exec_params["impact_bps_cap_mult"],
        daily_loss_stop=exec_params["daily_loss_stop"],
        monthly_drawdown_stop=exec_params["monthly_drawdown_stop"],
        stop_cooldown_days=exec_params["stop_cooldown_days"],
        regime_filter_enabled=exec_params["regime_filter_enabled"],
        regime_ma_window=exec_params["regime_ma_window"],
        regime_vol_window=exec_params["regime_vol_window"],
        regime_high_vol_threshold=exec_params["regime_high_vol_threshold"],
        regime_defensive_exposure=exec_params["regime_defensive_exposure"],
        score_mom_short=exec_params["score_mom_short"],
        score_mom_long=exec_params["score_mom_long"],
        score_vol_window=exec_params["score_vol_window"],
        score_dd_window=exec_params["score_dd_window"],
        score_w_mom_short=exec_params["score_w_mom_short"],
        score_w_mom_long=exec_params["score_w_mom_long"],
        score_w_low_vol=exec_params["score_w_low_vol"],
        score_w_low_drawdown=exec_params["score_w_low_drawdown"],
        timing_switch_enabled=exec_params["timing_switch_enabled"],
        trend_short_ma=exec_params["trend_short_ma"],
        trend_long_ma=exec_params["trend_long_ma"],
        trend_gate_threshold=exec_params["trend_gate_threshold"],
        trend_amplify_threshold=exec_params["trend_amplify_threshold"],
        trend_amplify_mult=exec_params["trend_amplify_mult"],
        trend_defensive_scale=exec_params["trend_defensive_scale"],
        adaptive_top_n_enabled=exec_params["adaptive_top_n_enabled"],
        top_n_strong=exec_params["top_n_strong"],
        top_n_neutral=exec_params["top_n_neutral"],
        top_n_weak=exec_params["top_n_weak"],
        trend_strong_threshold=exec_params["trend_strong_threshold"],
        trend_weak_threshold=exec_params["trend_weak_threshold"],
        buy_threshold=exec_params["buy_threshold"],
        sell_threshold=exec_params["sell_threshold"],
        entry_confirm_periods=exec_params["entry_confirm_periods"],
        min_hold_rebalance_periods=exec_params["min_hold_rebalance_periods"],
        reentry_cooldown_periods=exec_params["reentry_cooldown_periods"],
        sentiment_exposure_cap=sentiment_exposure_cap,
        init_cash=exec_params["init_cash"],
        max_trade_amount_ratio=exec_params["max_trade_amount_ratio"],
        asset_params=asset_params,
        asset_class_map=asset_class_map,
        class_max_positions=class_constraints.get("class_max_positions") or {},
        allowed_classes=class_constraints.get("allowed_classes"),
        class_exposure_caps=class_constraints.get("class_exposure_caps") or {},
    )

    ai_referee_csv_path = None
    ai_referee_md_path = None
    ab_csv_path = None
    ab_json_path = None
    ab_md_path = None
    ab_decision = "BASELINE_KEEP"
    ai_result = None

    if exec_params["ai_referee_enabled"]:
        try:
            llm_cfg = (cfg.get("llm") or {}) if isinstance(cfg, dict) else {}
            if llm_cfg.get("base_url"):
                os.environ["LLM_BASE_URL"] = str(llm_cfg.get("base_url"))
            if llm_cfg.get("api_key"):
                os.environ["LLM_API_KEY"] = str(llm_cfg.get("api_key"))
            if llm_cfg.get("model"):
                os.environ["LLM_MODEL"] = str(llm_cfg.get("model"))

            bench_close = load_close_matrix(codes=[benchmark_code], source="baostock")[benchmark_code]
            ai_ref_df, ai_ref_sent = build_ai_referee_signals(
                benchmark_close=bench_close,
                rebalance="W-FRI",
                llm_enabled=True,
                max_llm_points=exec_params["ai_referee_max_points"],
            )
            ai_referee_csv_path, ai_referee_md_path = save_ai_referee_outputs(ai_ref_df, prefix="paper_rotation")

            sent_series = ai_ref_sent.reindex(baseline_result.daily_returns.index).ffill().fillna(0.0)
            ai_result = run_from_local_cache(
                codes=tradable_codes,
                source="baostock",
                rebalance="W-FRI",
                top_n=exec_params["top_n"],
                fee_bps=exec_params["fee_bps"],
                slippage_bps=exec_params["slippage_bps"],
                min_score=exec_params["min_score"],
                sentiment=sent_series,
                benchmark_code=benchmark_code,
                max_turnover=exec_params["max_turnover"],
                use_risk_parity=True,
                vol_lookback=exec_params["vol_lookback"],
                target_vol_ann=exec_params["target_vol_ann"],
                vol_target_lookback=20,
                max_leverage=1.0,
                drawdown_stop=exec_params["drawdown_stop"],
                drawdown_recovery=exec_params["drawdown_recovery"],
                dd_cooldown_days=exec_params["dd_cooldown_days"],
                dd_rearm_days=exec_params["dd_rearm_days"],
                impact_bps=exec_params["impact_bps"],
                impact_power=exec_params["impact_power"],
                impact_bps_cap_mult=exec_params["impact_bps_cap_mult"],
                daily_loss_stop=exec_params["daily_loss_stop"],
                monthly_drawdown_stop=exec_params["monthly_drawdown_stop"],
                stop_cooldown_days=exec_params["stop_cooldown_days"],
                regime_filter_enabled=exec_params["regime_filter_enabled"],
                regime_ma_window=exec_params["regime_ma_window"],
                regime_vol_window=exec_params["regime_vol_window"],
                regime_high_vol_threshold=exec_params["regime_high_vol_threshold"],
                regime_defensive_exposure=exec_params["regime_defensive_exposure"],
                score_mom_short=exec_params["score_mom_short"],
                score_mom_long=exec_params["score_mom_long"],
                score_vol_window=exec_params["score_vol_window"],
                score_dd_window=exec_params["score_dd_window"],
                score_w_mom_short=exec_params["score_w_mom_short"],
                score_w_mom_long=exec_params["score_w_mom_long"],
                score_w_low_vol=exec_params["score_w_low_vol"],
                score_w_low_drawdown=exec_params["score_w_low_drawdown"],
                buy_threshold=exec_params["buy_threshold"],
                sell_threshold=exec_params["sell_threshold"],
                sentiment_exposure_cap=sentiment_exposure_cap,
                entry_confirm_periods=exec_params["entry_confirm_periods"],
                min_hold_rebalance_periods=exec_params["min_hold_rebalance_periods"],
                reentry_cooldown_periods=exec_params["reentry_cooldown_periods"],
                timing_switch_enabled=exec_params["timing_switch_enabled"],
                trend_short_ma=exec_params["trend_short_ma"],
                trend_long_ma=exec_params["trend_long_ma"],
                trend_gate_threshold=exec_params["trend_gate_threshold"],
                trend_amplify_threshold=exec_params["trend_amplify_threshold"],
                trend_amplify_mult=exec_params["trend_amplify_mult"],
                trend_defensive_scale=exec_params["trend_defensive_scale"],
                adaptive_top_n_enabled=exec_params["adaptive_top_n_enabled"],
                top_n_strong=exec_params["top_n_strong"],
                top_n_neutral=exec_params["top_n_neutral"],
                top_n_weak=exec_params["top_n_weak"],
                trend_strong_threshold=exec_params["trend_strong_threshold"],
                trend_weak_threshold=exec_params["trend_weak_threshold"],
                init_cash=exec_params["init_cash"],
                max_trade_amount_ratio=exec_params["max_trade_amount_ratio"],
                asset_params=asset_params,
                asset_class_map=asset_class_map,
                class_max_positions=class_constraints.get("class_max_positions") or {},
                allowed_classes=class_constraints.get("allowed_classes"),
                class_exposure_caps=class_constraints.get("class_exposure_caps") or {},
            )
            ab_csv_path, ab_json_path, ab_md_path = save_ab_compare(
                baseline_metrics=baseline_result.metrics,
                ai_metrics=ai_result.metrics,
                prefix="paper_rotation",
            )
            try:
                ab_payload = json.loads((ab_json_path).read_text(encoding="utf-8"))
                ab_decision = str(ab_payload.get("decision", "BASELINE_KEEP"))
            except Exception:
                ab_decision = "BASELINE_KEEP"
        except Exception as e:
            print(f"[warn] AI裁判层失败，回退baseline: {e}")

    result = baseline_result
    if ai_result is not None and exec_params["ai_referee_apply_if_better"] and ab_decision == "AI_REFEREE_ON":
        result = ai_result

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

    wf_train_days = int(cfg_backtest.get("wf_train_days", 120))
    wf_test_days = int(cfg_backtest.get("wf_test_days", 20))
    wf_step_days = int(cfg_backtest.get("wf_step_days", 20))

    wf_table, wf_returns = run_walk_forward_from_local_cache(
        codes=tradable_codes,
        source="baostock",
        rebalance="W-FRI",
        fee_bps=exec_params["fee_bps"],
        slippage_bps=exec_params["slippage_bps"],
        train_days=wf_train_days,
        test_days=wf_test_days,
        step_days=wf_step_days,
        top_n_grid=[1, 2],
        min_score_grid=[-0.2, -0.1, 0.0],
        asset_params=asset_params,
    )
    wf_table_path = ROOT / "reports" / "paper_rotation_walk_forward.csv"
    wf_equity_path = ROOT / "reports" / "paper_rotation_walk_forward_equity.csv"
    wf_table.to_csv(wf_table_path, index=False)
    ((1.0 + wf_returns).cumprod().rename("wf_equity")).to_csv(wf_equity_path, index=True)

    stability_df = run_parameter_stability_from_local_cache(
        codes=tradable_codes,
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
        asset_params=asset_params,
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

    amount_df = load_amount_matrix(codes=tradable_codes, source="baostock").reindex(result.weights.index).fillna(0.0)
    fills = simulate_paper_trades(
        result.weights,
        init_cash=exec_params["init_cash"],
        execution_time=exec_params["execution_time"],
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
        strategy_context={
            "dominant_strategy_class": exec_params.get("dominant_strategy_class", "unknown"),
            "dominant_backtest_template": exec_params.get("dominant_backtest_template", "unknown"),
            "gatekeeper_state": exec_params.get("gatekeeper_state", "unknown"),
        },
    )
    report_path = save_report(report_text, filename="paper_rotation_daily.md")

    qs_path = _try_generate_quantstats(result)
    baseline_stats = _try_run_backtestingpy_baseline(code=tradable_codes[0], cash=exec_params["init_cash"])

    baseline_text = ""
    if baseline_stats is not None:
        baseline_text = f"\n- Backtesting.py基准: {baseline_stats}"

    qs_text = ""
    if qs_path is not None:
        qs_text = f"\n- QuantStats报告: {qs_path}"

    quality_counts = quality_df["severity"].value_counts().to_dict() if not quality_df.empty else {}
    window_snapshot = _build_window_snapshot(result=result, benchmark_code=benchmark_code, source="baostock")
    macro_alignment = _build_macro_alignment_audit(macro_provenance=macro_provenance, window_snapshot=window_snapshot)
    macro_alignment_path = ROOT / "reports" / "paper_rotation_macro_alignment_audit.json"
    macro_alignment_path.write_text(json.dumps(macro_alignment, ensure_ascii=False, indent=2), encoding="utf-8")
    validity = _backtest_validity_score(
        window_snapshot=window_snapshot,
        wf_table=wf_table,
        quality_df=quality_df,
        cost_total=float(result.metrics.get("cost_total", 0.0)),
    )
    dashboard_path = _save_backtest_dashboard_html(
        result=result,
        metrics=result.metrics,
        window_snapshot=window_snapshot,
        validity=validity,
        exec_params=exec_params,
    )

    best_stab = stability_df.iloc[0].to_dict() if not stability_df.empty else {}
    best_benchmark = benchmark_df.iloc[0].to_dict() if not benchmark_df.empty else {}
    fill_cost_total = float(fills_df["est_cost"].sum()) if not fills_df.empty else 0.0
    fill_impact_total = float(fills_df["impact_cost"].sum()) if not fills_df.empty else 0.0
    backtest_cost_cash = float(result.metrics.get("cost_total", 0.0)) * float(exec_params["init_cash"])
    backtest_impact_cash = float(result.metrics.get("cost_impact", 0.0)) * float(exec_params["init_cash"])

    target_eval = {
        "annual_return": {
            "target": float(cfg_targets.get("annual_return_min", 0.08)),
            "observed": float(result.metrics.get("annual_return", 0.0)),
            "pass": float(result.metrics.get("annual_return", 0.0)) >= float(cfg_targets.get("annual_return_min", 0.08)),
        },
        "max_drawdown": {
            "target": float(cfg_targets.get("max_drawdown_min", -0.20)),
            "observed": float(result.metrics.get("max_drawdown", 0.0)),
            "pass": float(result.metrics.get("max_drawdown", 0.0)) >= float(cfg_targets.get("max_drawdown_min", -0.20)),
        },
        "sharpe": {
            "target": float(cfg_targets.get("sharpe_min", 0.8)),
            "observed": float(result.metrics.get("sharpe", 0.0)),
            "pass": float(result.metrics.get("sharpe", 0.0)) >= float(cfg_targets.get("sharpe_min", 0.8)),
        },
    }
    target_fail_items = [k for k, v in target_eval.items() if not bool(v.get("pass", False))]

    summary = (
        f"纸盘运行完成（风险状态: {risk_review.get('status', 'PASS')}）\n"
        f"- 执行参数: {exec_params}\n"
        f"- 总闸门: state={exec_params.get('gatekeeper_state')}, score={exec_params.get('gatekeeper_score'):.4f}, actions={exec_params.get('gatekeeper_actions')}\n"
        f"- 宏观数据口径: source_mode={macro_provenance.get('source_mode')}, historical_news_series={macro_provenance.get('has_historical_news_series')}, gatekeeper_driver={macro_provenance.get('backtest_gatekeeper_driver')}\n"
        f"- 宏观代理序列: enabled={macro_sent_summary.get('enabled')}, coverage={macro_sent_summary.get('coverage_ratio')}, state_counts={macro_sent_summary.get('state_counts')}, shift_days={macro_sent_summary.get('no_lookahead_shift_days')}\n"
        f"- 宏观风险缩放上限: sentiment_exposure_cap_effective={sentiment_exposure_cap}\n"
        f"- 宏观对齐检查: status={macro_alignment.get('realism_status')}, coverage={((macro_alignment.get('macro_history_window') or {}).get('coverage'))}\n"
        f"- 分类硬约束: {class_constraints}\n"
        f"- 分型主类: {exec_params.get('dominant_strategy_class')} | 模板: {exec_params.get('dominant_backtest_template')}\n"
        f"- 分型统计: {class_snapshot.get('class_counts', {})}\n"
        f"- 标的池: discovered={len(codes)}, tradable={len(tradable_codes)}, excluded_fail={len(excluded_fail_codes)}, exclude_fail_enabled={exclude_fail_from_backtest}\n"
        f"- 质量FAIL剔除列表: {excluded_fail_path}\n"
        f"- 审批参数文件: {approved_path}\n"
        f"- 基准: {benchmark_code}\n"
        f"- 数据质量: {quality_counts}\n"
        f"- 数据质量CSV: {quality_csv_path}\n"
        f"- 数据质量MD: {quality_md_path}\n"
        f"- 指标: {result.metrics}\n"
        f"- 净值文件: {eq_path}\n"
        f"- 权重文件: {wt_path}\n"
        f"- 成交文件: {fills_path}\n"
        f"- 模拟总资金: {exec_params['init_cash']:.2f}\n"
        f"- 模拟执行时间: {exec_params['execution_time']}（仅交易时段）\n"
        f"- 单日成交额占比上限: {exec_params['max_trade_amount_ratio']:.2%}\n"
        f"- 风险预算缩放: {exposure_path}\n"
        f"- 成本模型: fee={exec_params['fee_bps']}bps, slippage={exec_params['slippage_bps']}bps, impact={exec_params['impact_bps']}bps, power={exec_params['impact_power']}\n"
        f"- 停盘阈值: total_dd={exec_params['drawdown_stop']}, dd_recovery={exec_params['drawdown_recovery']}, dd_cooldown={exec_params['dd_cooldown_days']}, dd_rearm={exec_params['dd_rearm_days']}, daily={exec_params['daily_loss_stop']}, monthly_dd={exec_params['monthly_drawdown_stop']}, cooldown={exec_params['stop_cooldown_days']}\n"
        f"- 市场状态过滤: enabled={exec_params['regime_filter_enabled']}, ma={exec_params['regime_ma_window']}, vol_window={exec_params['regime_vol_window']}, vol_th={exec_params['regime_high_vol_threshold']}, def_exp={exec_params['regime_defensive_exposure']}\n"
        f"- 选股打分参数: mom=({exec_params['score_mom_short']},{exec_params['score_mom_long']}), vol_win={exec_params['score_vol_window']}, dd_win={exec_params['score_dd_window']}, w=({exec_params['score_w_mom_short']},{exec_params['score_w_mom_long']},{exec_params['score_w_low_vol']},{exec_params['score_w_low_drawdown']})\n"
        f"- 择时开关: enabled={exec_params['timing_switch_enabled']}, ma=({exec_params['trend_short_ma']},{exec_params['trend_long_ma']}), gate={exec_params['trend_gate_threshold']}, amp_th={exec_params['trend_amplify_threshold']}, amp_mult={exec_params['trend_amplify_mult']}, def_scale={exec_params['trend_defensive_scale']}\n"
        f"- 自适应TopN: enabled={exec_params['adaptive_top_n_enabled']}, strong/neutral/weak=({exec_params['top_n_strong']},{exec_params['top_n_neutral']},{exec_params['top_n_weak']}), trend_th=({exec_params['trend_weak_threshold']},{exec_params['trend_strong_threshold']})\n"
        f"- 反复打脸抑制: buy_th={exec_params['buy_threshold']}, sell_th={exec_params['sell_threshold']}, entry_confirm={exec_params['entry_confirm_periods']}, min_hold={exec_params['min_hold_rebalance_periods']}, reentry_cooldown={exec_params['reentry_cooldown_periods']}\n"
        f"- AI裁判层: enabled={exec_params['ai_referee_enabled']}, exposure_cap={exec_params['ai_referee_exposure_cap']}, max_points={exec_params['ai_referee_max_points']}, apply_if_better={exec_params['ai_referee_apply_if_better']}, ab_decision={ab_decision}\n"
        f"- 回测总成本: 比例={result.metrics.get('cost_total', 0.0):.4f}，金额≈{backtest_cost_cash:.2f}（冲击成本≈{backtest_impact_cash:.2f}）\n"
        f"- 停盘触发次数: daily={int(result.metrics.get('stop_trigger_daily', 0))}, monthly={int(result.metrics.get('stop_trigger_monthly', 0))}, total_dd={int(result.metrics.get('stop_trigger_total_dd', 0))}\n"
        f"- 模拟目标评估: fail_items={target_fail_items}, detail={target_eval}\n"
        f"- 模拟成交总成本: {fill_cost_total:.2f}（冲击成本 {fill_impact_total:.2f}）\n"
        f"- 回测区间: {window_snapshot.get('start')} ~ {window_snapshot.get('end')} | 交易日={window_snapshot.get('trading_days')} | 自然日={window_snapshot.get('calendar_days')}\n"
        f"- 行情覆盖: {window_snapshot.get('regime_days')}\n"
        f"- 回测有效性评分: {validity.get('score')}/100 | checks={validity.get('checks')}\n"
        f"- WalkForward配置: train={wf_train_days}, test={wf_test_days}, step={wf_step_days}, windows={validity.get('wf_windows')}\n"
        f"- 本地回测看板: {dashboard_path}\n"
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
        f"- 分型快照CSV: {class_csv_path}\n"
        f"- 分型快照JSON: {class_snapshot_path}\n"
        f"- 总闸门快照JSON: {gate_snapshot_path}\n"
        f"- 宏观数据口径JSON: {macro_provenance_path}\n"
        f"- 宏观代理序列CSV: {macro_regime_path}\n"
        f"- 宏观对齐审计JSON: {macro_alignment_path}\n"
        f"- 新策略生效快照JSON: {strategy_effective_snapshot_path}\n"
        f"- 风控检查JSON: {risk_json_path}\n"
        f"- 风控检查MD: {risk_md_path}\n"
        f"- 风控失败项: {risk_review.get('fail_items', [])}\n"
        f"- AI研究报告JSON: {ai_json_path}\n"
        f"- AI研究报告MD: {ai_md_path}\n"
        f"- AI裁判CSV: {ai_referee_csv_path}\n"
        f"- AI裁判MD: {ai_referee_md_path}\n"
        f"- A/B对比CSV: {ab_csv_path}\n"
        f"- A/B对比JSON: {ab_json_path}\n"
        f"- A/B对比MD: {ab_md_path}\n"
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

    severe_anomaly = False
    severe_reasons = []
    if risk_review.get("status") == "FAIL":
        severe_anomaly = True
        severe_reasons.append("risk_guard_fail")
    if int(result.metrics.get("stop_trigger_daily", 0)) > 0 or int(result.metrics.get("stop_trigger_monthly", 0)) > 0:
        severe_anomaly = True
        severe_reasons.append("stop_guard_triggered")
    if int((quality_df["severity"] == "FAIL").sum()) >= int(cfg_notify.get("quality_fail_alert_count", 6)):
        severe_anomaly = True
        severe_reasons.append("quality_fail_count_high")

    push_daily_detail = bool(cfg_notify.get("push_daily_detail", False))
    push_on_anomaly = bool(cfg_notify.get("push_on_anomaly", True))
    push_trade_changes = bool(cfg_notify.get("push_trade_changes", True))
    trade_change_min_weight = float(cfg_notify.get("trade_change_min_weight", 0.001))
    trade_change_min_notional = float(cfg_notify.get("trade_change_min_notional", 10.0))

    trade_buys = []
    trade_sells = []
    latest_trade_date = None
    if not fills_df.empty and "date" in fills_df.columns:
        fills_df["date"] = pd.to_datetime(fills_df["date"], errors="coerce")
        fills_df["weight_change"] = pd.to_numeric(fills_df.get("weight_change"), errors="coerce")
        fills_df["trade_notional"] = pd.to_numeric(fills_df.get("trade_notional"), errors="coerce")
        fills_df = fills_df.dropna(subset=["date", "weight_change"]).copy()
        if not fills_df.empty:
            latest_trade_date = pd.Timestamp(fills_df["date"].max()).normalize()
            day_fills = fills_df[fills_df["date"].dt.normalize() == latest_trade_date].copy()
            day_fills = day_fills[
                (day_fills["weight_change"].abs() >= trade_change_min_weight)
                & (day_fills["trade_notional"].abs() >= trade_change_min_notional)
            ]
            if not day_fills.empty:
                by_symbol = day_fills.groupby("symbol", as_index=False)[["weight_change", "trade_notional"]].sum()
                buys = by_symbol[by_symbol["weight_change"] > 0].sort_values("weight_change", ascending=False)
                sells = by_symbol[by_symbol["weight_change"] < 0].sort_values("weight_change", ascending=True)
                trade_buys = [(str(r["symbol"]), float(r["weight_change"]), float(r["trade_notional"])) for _, r in buys.iterrows()]
                trade_sells = [(str(r["symbol"]), float(-r["weight_change"]), float(abs(r["trade_notional"]))) for _, r in sells.iterrows()]

    if push_daily_detail or (push_on_anomaly and severe_anomaly):
        msg = (
            f"日度结果: 年化={result.metrics.get('annual_return', 0.0):.2%}, "
            f"回撤={result.metrics.get('max_drawdown', 0.0):.2%}, "
            f"Sharpe={result.metrics.get('sharpe', 0.0):.2f}, "
            f"Alpha={result.metrics.get('alpha_annual', 0.0):.2%}, "
            f"成本={result.metrics.get('cost_total', 0.0):.4f}"
        )
        if severe_anomaly:
            msg += f"\n异常: {severe_reasons}"
        push_dm(msg)

    if push_trade_changes and (trade_buys or trade_sells):
        daily_pnl = float(result.equity.iloc[-1] - result.equity.iloc[-2]) if len(result.equity) >= 2 else 0.0
        total_pnl = float(result.equity.iloc[-1] - exec_params["init_cash"]) if len(result.equity) >= 1 else 0.0
        total_ret = float(result.equity.iloc[-1] / max(exec_params["init_cash"], 1e-12) - 1.0) if len(result.equity) >= 1 else 0.0

        date_str = str(latest_trade_date.date()) if latest_trade_date is not None else str(pd.Timestamp.today().date())
        signature_payload = {
            "date": date_str,
            "buys": [(c, round(w, 6), round(n, 2)) for c, w, n in trade_buys],
            "sells": [(c, round(w, 6), round(n, 2)) for c, w, n in trade_sells],
        }
        signature = json.dumps(signature_payload, ensure_ascii=False, sort_keys=True)
        alert_state = _load_trade_alert_state()
        if str(alert_state.get("last_signature", "")) == signature:
            return

        trade_text = (
            "调仓提醒\n"
            f"- 交易日: {date_str}\n"
            f"- 买入: {', '.join([f'{c}(+{w:.1%}, 约{n:.0f}元)' for c, w, n in trade_buys[:6]]) if trade_buys else '无'}\n"
            f"- 卖出: {', '.join([f'{c}(-{w:.1%}, 约{n:.0f}元)' for c, w, n in trade_sells[:6]]) if trade_sells else '无'}\n"
            f"- 当日盈亏: {daily_pnl:+.2f} 元\n"
            f"- 累计盈亏: {total_pnl:+.2f} 元（{total_ret:+.2%}）"
        )
        push_dm(trade_text)
        _save_trade_alert_state({"last_signature": signature, "updated_at": pd.Timestamp.now().isoformat()})


if __name__ == "__main__":
    main()
