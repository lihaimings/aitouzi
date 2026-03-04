from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd

from src.backtest.metrics import compute_performance_metrics
from src.portfolio import cap_weight_and_normalize

DATA_DIR = Path(__file__).resolve().parents[2] / "data"


@dataclass
class BacktestResult:
    equity: pd.Series
    daily_returns: pd.Series
    weights: pd.DataFrame
    score: pd.DataFrame
    metrics: Dict[str, float]
    benchmark_returns: Optional[pd.Series] = None
    exposure_scale: Optional[pd.Series] = None
    fee_slippage_cost: Optional[pd.Series] = None
    impact_cost: Optional[pd.Series] = None
    total_cost: Optional[pd.Series] = None


def _load_etf_field(code: str, field: str, source: str = "baostock") -> pd.Series:
    candidates = [
        DATA_DIR / f"etf_{code}_{source}.csv",
        DATA_DIR / f"etf_{code}.csv",
        DATA_DIR / f"etf_{code}_{source}.parquet",
        DATA_DIR / f"etf_{code}.parquet",
    ]
    target = next((p for p in candidates if p.exists()), None)
    if target is None:
        raise FileNotFoundError(f"找不到ETF数据文件: {code}, source={source}")

    if target.suffix == ".parquet":
        df = pd.read_parquet(target)
    else:
        df = pd.read_csv(target)

    if "date" not in df.columns or field not in df.columns:
        raise ValueError(f"数据缺少必要列(date/{field}): {target}")

    df["date"] = pd.to_datetime(df["date"])
    series = pd.to_numeric(df.set_index("date")[field], errors="coerce").astype(float).sort_index()
    series.name = code
    return series


def _load_etf_close(code: str, source: str = "baostock") -> pd.Series:
    return _load_etf_field(code=code, field="close", source=source)


def _load_etf_amount(code: str, source: str = "baostock") -> pd.Series:
    return _load_etf_field(code=code, field="amount", source=source)


def load_close_matrix(codes: Iterable[str], source: str = "baostock") -> pd.DataFrame:
    prices = [_load_etf_close(c, source=source) for c in codes]
    mat = pd.concat(prices, axis=1).sort_index().dropna(how="all")
    return mat.ffill().dropna(how="any")


def load_amount_matrix(codes: Iterable[str], source: str = "baostock") -> pd.DataFrame:
    amount_list = []
    for c in codes:
        try:
            amount_list.append(_load_etf_amount(c, source=source))
        except Exception:
            pass

    if not amount_list:
        return pd.DataFrame()

    mat = pd.concat(amount_list, axis=1).sort_index()
    return mat.fillna(0.0)


def compute_rotation_score(
    close_df: pd.DataFrame,
    mom_short: int = 20,
    mom_long: int = 60,
) -> pd.DataFrame:
    r1 = close_df.pct_change(mom_short)
    r2 = close_df.pct_change(mom_long)

    # 缺少长窗口历史时，自动退化为可用窗口的加权均值
    w1, w2 = 0.6, 0.4
    numerator = r1.fillna(0.0) * w1 + r2.fillna(0.0) * w2
    denominator = (~r1.isna()).astype(float) * w1 + (~r2.isna()).astype(float) * w2
    score = numerator.div(denominator.where(denominator > 0))
    return score


def build_target_weights(
    score: pd.DataFrame,
    rebalance: str = "W-FRI",
    top_n: int = 2,
    min_score: float = -1.0,
    sentiment: Optional[pd.Series] = None,
    close_df: Optional[pd.DataFrame] = None,
    use_risk_parity: bool = False,
    vol_lookback: int = 20,
) -> pd.DataFrame:
    if top_n <= 0:
        raise ValueError("top_n 必须大于 0")

    rebalance_idx = score.resample(rebalance).last().index
    weights = pd.DataFrame(0.0, index=score.index, columns=score.columns)

    for dt in rebalance_idx:
        if dt not in score.index:
            prior = score.index[score.index <= dt]
            if len(prior) == 0:
                continue
            dt = prior[-1]

        row = score.loc[dt].dropna()
        if row.empty:
            continue

        select = row[row >= min_score].nlargest(top_n)
        if len(select) == 0:
            continue

        risk_scale = 1.0
        if sentiment is not None:
            s = sentiment.reindex(score.index).ffill().fillna(0.0)
            risk_scale = float(np.clip((s.loc[dt] + 1.0) / 2.0, 0.0, 1.0))

        w = pd.Series(0.0, index=score.columns)
        if use_risk_parity and close_df is not None:
            vol = close_df.pct_change().rolling(vol_lookback).std().reindex(score.index).loc[dt, select.index]
            inv_vol = 1.0 / vol.replace(0, np.nan)
            inv_vol = inv_vol.replace([np.inf, -np.inf], np.nan).dropna()
            if len(inv_vol) > 0:
                rp_w = inv_vol / inv_vol.sum()
                w[rp_w.index] = risk_scale * rp_w.values
            else:
                w[select.index] = risk_scale / len(select)
        else:
            w[select.index] = risk_scale / len(select)
        weights.loc[dt] = w

    return weights.replace(0, np.nan).ffill().fillna(0.0)


def _apply_turnover_cap(weights: pd.DataFrame, max_turnover: float) -> pd.DataFrame:
    if max_turnover <= 0:
        return weights * 0.0

    capped = pd.DataFrame(index=weights.index, columns=weights.columns, dtype=float)
    prev = pd.Series(0.0, index=weights.columns)

    for dt, row in weights.iterrows():
        target = row.fillna(0.0).astype(float)
        delta = target - prev
        turnover = float(delta.abs().sum())
        if turnover > max_turnover and turnover > 0:
            scale = max_turnover / turnover
            target = prev + delta * scale
        capped.loc[dt] = target
        prev = target

    return capped.fillna(0.0)


def run_rotation_backtest(
    close_df: pd.DataFrame,
    rebalance: str = "W-FRI",
    top_n: int = 2,
    fee_bps: float = 5.0,
    slippage_bps: float = 5.0,
    min_score: float = -1.0,
    sentiment: Optional[pd.Series] = None,
    init_cash: float = 1.0,
    benchmark_returns: Optional[pd.Series] = None,
    max_turnover: Optional[float] = None,
    use_risk_parity: bool = False,
    vol_lookback: int = 20,
    target_vol_ann: Optional[float] = None,
    vol_target_lookback: int = 20,
    max_leverage: float = 1.0,
    drawdown_stop: Optional[float] = None,
    dd_cooldown_days: int = 0,
    amount_df: Optional[pd.DataFrame] = None,
    impact_bps: float = 0.0,
    impact_power: float = 0.5,
    impact_bps_cap_mult: float = 5.0,
    daily_loss_stop: Optional[float] = None,
    monthly_drawdown_stop: Optional[float] = None,
    stop_cooldown_days: int = 0,
    benchmark_close: Optional[pd.Series] = None,
    regime_filter_enabled: bool = False,
    regime_ma_window: int = 200,
    regime_vol_window: int = 20,
    regime_high_vol_threshold: float = 0.02,
    regime_defensive_exposure: float = 0.3,
) -> BacktestResult:
    close_df = close_df.sort_index().ffill().dropna(how="any")
    ret = close_df.pct_change().fillna(0.0)

    score = compute_rotation_score(close_df)
    weights = build_target_weights(
        score=score,
        rebalance=rebalance,
        top_n=top_n,
        min_score=min_score,
        sentiment=sentiment,
        close_df=close_df,
        use_risk_parity=use_risk_parity,
        vol_lookback=vol_lookback,
    )
    weights = cap_weight_and_normalize(weights, max_single_weight=0.6)

    if max_turnover is not None:
        weights = _apply_turnover_cap(weights, max_turnover=max_turnover)

    cost_rate = (fee_bps + slippage_bps) / 10000.0
    impact_rate_base = max(0.0, impact_bps) / 10000.0
    impact_power = max(0.0, float(impact_power))
    impact_rate_cap = max(impact_rate_base, impact_rate_base * max(1.0, float(impact_bps_cap_mult)))

    if amount_df is not None and not amount_df.empty:
        amount_df = amount_df.reindex(ret.index).fillna(0.0)
    else:
        amount_df = pd.DataFrame(index=ret.index, columns=ret.columns, data=0.0)

    bench_close = None
    bench_ma = None
    bench_vol = None
    if benchmark_close is not None:
        bench_close = pd.Series(benchmark_close).reindex(ret.index).ffill()
        bench_ma = bench_close.rolling(max(5, int(regime_ma_window))).mean()
        bench_vol = bench_close.pct_change().rolling(max(5, int(regime_vol_window))).std()

    # 风险预算：波动目标 + 回撤保护（按日动态缩放仓位）
    exposure_scale = pd.Series(1.0, index=weights.index, dtype=float)
    eff_weights = pd.DataFrame(0.0, index=weights.index, columns=weights.columns)

    peak_equity = float(init_cash)
    cur_equity = float(init_cash)
    cooldown = 0
    month_tag = None
    month_peak_equity = float(init_cash)
    net_hist: List[float] = []
    stop_trigger_daily = 0
    stop_trigger_monthly = 0
    stop_trigger_total = 0

    prev_eff_target = pd.Series(0.0, index=weights.columns)
    prev_eff_exec = pd.Series(0.0, index=weights.columns)

    gross_list: List[float] = []
    turnover_list: List[float] = []
    fee_slippage_cost_list: List[float] = []
    impact_cost_list: List[float] = []
    net_list: List[float] = []

    for dt in weights.index:
        base_target = weights.loc[dt].fillna(0.0).astype(float)
        cur_month = pd.Timestamp(dt).to_period("M")
        if month_tag is None or cur_month != month_tag:
            month_tag = cur_month
            month_peak_equity = float(cur_equity)

        scale = 1.0

        if regime_filter_enabled and bench_close is not None and bench_ma is not None and bench_vol is not None:
            is_bear = bool(bench_close.loc[dt] < bench_ma.loc[dt]) if pd.notna(bench_ma.loc[dt]) else False
            is_high_vol = bool(bench_vol.loc[dt] >= float(regime_high_vol_threshold)) if pd.notna(bench_vol.loc[dt]) else False
            if is_bear or is_high_vol:
                scale *= float(np.clip(regime_defensive_exposure, 0.0, 1.0))

        if target_vol_ann is not None and len(net_hist) >= max(vol_target_lookback, 2):
            realized = float(np.std(net_hist[-vol_target_lookback:], ddof=0) * np.sqrt(252))
            if realized > 1e-12:
                scale = float(np.clip(target_vol_ann / realized, 0.0, max_leverage))
            else:
                scale = float(max_leverage)

        if drawdown_stop is not None:
            cur_dd = cur_equity / max(peak_equity, 1e-12) - 1.0
            if cooldown > 0:
                scale = 0.0
                cooldown -= 1
            elif cur_dd <= drawdown_stop:
                scale = 0.0
                cooldown = max(dd_cooldown_days - 1, 0)
                stop_trigger_total += 1

        if daily_loss_stop is not None and len(net_hist) > 0 and net_hist[-1] <= daily_loss_stop:
            scale = 0.0
            cooldown = max(cooldown, max(stop_cooldown_days - 1, 0))
            stop_trigger_daily += 1

        if monthly_drawdown_stop is not None:
            month_dd = cur_equity / max(month_peak_equity, 1e-12) - 1.0
            if month_dd <= monthly_drawdown_stop:
                scale = 0.0
                cooldown = max(cooldown, max(stop_cooldown_days - 1, 0))
                stop_trigger_monthly += 1

        exposure_scale.loc[dt] = scale
        eff_target = base_target * scale
        eff_weights.loc[dt] = eff_target

        daily_ret = ret.loc[dt].fillna(0.0)
        gross_t = float((prev_eff_exec * daily_ret).sum())
        delta_w = (eff_target - prev_eff_target).abs()
        turnover_t = float(delta_w.sum())
        fee_slippage_cost_t = turnover_t * cost_rate

        amount_row = amount_df.loc[dt].reindex(delta_w.index).fillna(0.0).astype(float)
        amount_sum = float(amount_row.sum())
        if impact_rate_base > 0 and amount_sum > 0:
            liq_share = amount_row / amount_sum
            ratio = delta_w / (liq_share + 1e-8)
            impact_rate_vec = (ratio.clip(lower=0.0) ** impact_power) * impact_rate_base
            impact_rate_vec = impact_rate_vec.clip(upper=impact_rate_cap)
            impact_cost_t = float((delta_w * impact_rate_vec).sum())
        else:
            impact_cost_t = 0.0

        total_cost_t = fee_slippage_cost_t + impact_cost_t
        net_t = gross_t - total_cost_t

        gross_list.append(gross_t)
        turnover_list.append(turnover_t)
        fee_slippage_cost_list.append(fee_slippage_cost_t)
        impact_cost_list.append(impact_cost_t)
        net_list.append(net_t)

        cur_equity *= (1.0 + net_t)
        peak_equity = max(peak_equity, cur_equity)
        month_peak_equity = max(month_peak_equity, cur_equity)
        net_hist.append(net_t)

        prev_eff_target = eff_target
        prev_eff_exec = eff_target

    gross = pd.Series(gross_list, index=weights.index, name="gross")
    turnover = pd.Series(turnover_list, index=weights.index, name="turnover")
    fee_slippage_cost = pd.Series(fee_slippage_cost_list, index=weights.index, name="fee_slippage_cost")
    impact_cost = pd.Series(impact_cost_list, index=weights.index, name="impact_cost")
    net = pd.Series(net_list, index=weights.index, name="net")
    costs = fee_slippage_cost + impact_cost

    weights = eff_weights
    equity = (1.0 + net).cumprod() * init_cash
    bench = None
    if benchmark_returns is not None:
        bench = pd.Series(benchmark_returns).reindex(net.index).fillna(0.0)

    metrics = compute_performance_metrics(equity=equity, daily_returns=net, benchmark_returns=bench)
    metrics["cost_total"] = float(costs.sum())
    metrics["cost_fee_slippage"] = float(fee_slippage_cost.sum())
    metrics["cost_impact"] = float(impact_cost.sum())
    metrics["avg_turnover"] = float(turnover.mean())
    metrics["stop_trigger_daily"] = float(stop_trigger_daily)
    metrics["stop_trigger_monthly"] = float(stop_trigger_monthly)
    metrics["stop_trigger_total_dd"] = float(stop_trigger_total)

    return BacktestResult(
        equity=equity,
        daily_returns=net,
        weights=weights,
        score=score,
        metrics=metrics,
        benchmark_returns=bench,
        exposure_scale=exposure_scale,
        fee_slippage_cost=fee_slippage_cost,
        impact_cost=impact_cost,
        total_cost=costs,
    )


def run_from_local_cache(
    codes: Iterable[str],
    source: str = "baostock",
    rebalance: str = "W-FRI",
    top_n: int = 2,
    fee_bps: float = 5.0,
    slippage_bps: float = 5.0,
    min_score: float = -1.0,
    sentiment: Optional[pd.Series] = None,
    benchmark_code: Optional[str] = None,
    max_turnover: Optional[float] = None,
    use_risk_parity: bool = False,
    vol_lookback: int = 20,
    target_vol_ann: Optional[float] = None,
    vol_target_lookback: int = 20,
    max_leverage: float = 1.0,
    drawdown_stop: Optional[float] = None,
    dd_cooldown_days: int = 0,
    impact_bps: float = 0.0,
    impact_power: float = 0.5,
    impact_bps_cap_mult: float = 5.0,
    daily_loss_stop: Optional[float] = None,
    monthly_drawdown_stop: Optional[float] = None,
    stop_cooldown_days: int = 0,
    regime_filter_enabled: bool = False,
    regime_ma_window: int = 200,
    regime_vol_window: int = 20,
    regime_high_vol_threshold: float = 0.02,
    regime_defensive_exposure: float = 0.3,
) -> BacktestResult:
    close = load_close_matrix(codes=codes, source=source)
    amount = load_amount_matrix(codes=codes, source=source).reindex(close.index).fillna(0.0)

    bench = None
    bench_close = None
    if benchmark_code is not None:
        bench_close = _load_etf_close(benchmark_code, source=source).reindex(close.index).ffill()
        bench = bench_close.pct_change().fillna(0.0)

    return run_rotation_backtest(
        close_df=close,
        rebalance=rebalance,
        top_n=top_n,
        fee_bps=fee_bps,
        slippage_bps=slippage_bps,
        min_score=min_score,
        sentiment=sentiment,
        benchmark_returns=bench,
        max_turnover=max_turnover,
        use_risk_parity=use_risk_parity,
        vol_lookback=vol_lookback,
        target_vol_ann=target_vol_ann,
        vol_target_lookback=vol_target_lookback,
        max_leverage=max_leverage,
        drawdown_stop=drawdown_stop,
        dd_cooldown_days=dd_cooldown_days,
        amount_df=amount,
        impact_bps=impact_bps,
        impact_power=impact_power,
        impact_bps_cap_mult=impact_bps_cap_mult,
        daily_loss_stop=daily_loss_stop,
        monthly_drawdown_stop=monthly_drawdown_stop,
        stop_cooldown_days=stop_cooldown_days,
        benchmark_close=bench_close,
        regime_filter_enabled=regime_filter_enabled,
        regime_ma_window=regime_ma_window,
        regime_vol_window=regime_vol_window,
        regime_high_vol_threshold=regime_high_vol_threshold,
        regime_defensive_exposure=regime_defensive_exposure,
    )


def run_walk_forward_from_local_cache(
    codes: Iterable[str],
    source: str = "baostock",
    rebalance: str = "W-FRI",
    fee_bps: float = 5.0,
    slippage_bps: float = 5.0,
    train_days: int = 60,
    test_days: int = 20,
    step_days: int = 20,
    top_n_grid: Optional[List[int]] = None,
    min_score_grid: Optional[List[float]] = None,
) -> Tuple[pd.DataFrame, pd.Series]:
    """
    滚动验证（walk-forward）：
    - 每个窗口先用训练集选参数（top_n / min_score）
    - 再冻结参数跑测试集
    - 输出每个窗口的验证结果和拼接后的测试期收益曲线
    """
    close = load_close_matrix(codes=codes, source=source)
    if len(close) < train_days + test_days + 5:
        raise ValueError("数据长度不足，无法进行walk-forward，请增加历史数据")

    top_n_grid = top_n_grid or [1, 2, 3]
    min_score_grid = min_score_grid or [-0.2, -0.1, 0.0]

    split_rows = []
    stitched_returns = []

    for start in range(0, len(close) - train_days - test_days + 1, step_days):
        train_slice = close.iloc[start : start + train_days]
        test_slice = close.iloc[start + train_days : start + train_days + test_days]

        best = None
        best_obj = -1e18

        for top_n in top_n_grid:
            for min_score in min_score_grid:
                res_train = run_rotation_backtest(
                    close_df=train_slice,
                    rebalance=rebalance,
                    top_n=top_n,
                    fee_bps=fee_bps,
                    slippage_bps=slippage_bps,
                    min_score=min_score,
                )
                obj = res_train.metrics["annual_return"] + 0.3 * res_train.metrics["max_drawdown"]
                if obj > best_obj:
                    best_obj = obj
                    best = (top_n, min_score)

        best_top_n, best_min_score = best
        res_test = run_rotation_backtest(
            close_df=test_slice,
            rebalance=rebalance,
            top_n=best_top_n,
            fee_bps=fee_bps,
            slippage_bps=slippage_bps,
            min_score=best_min_score,
        )

        split_rows.append(
            {
                "train_start": train_slice.index.min(),
                "train_end": train_slice.index.max(),
                "test_start": test_slice.index.min(),
                "test_end": test_slice.index.max(),
                "best_top_n": best_top_n,
                "best_min_score": best_min_score,
                "test_total_return": res_test.metrics["total_return"],
                "test_annual_return": res_test.metrics["annual_return"],
                "test_sharpe": res_test.metrics["sharpe"],
                "test_max_drawdown": res_test.metrics["max_drawdown"],
            }
        )
        stitched_returns.append(res_test.daily_returns)

    wf_table = pd.DataFrame(split_rows)
    wf_returns = pd.concat(stitched_returns).sort_index()
    wf_returns = wf_returns[~wf_returns.index.duplicated(keep="first")]
    return wf_table, wf_returns


def save_backtest_outputs(result: BacktestResult, prefix: str = "rotation") -> Tuple[Path, Path]:
    out_dir = Path(__file__).resolve().parents[2] / "reports"
    out_dir.mkdir(parents=True, exist_ok=True)

    equity_path = out_dir / f"{prefix}_equity.csv"
    weights_path = out_dir / f"{prefix}_weights.csv"

    result.equity.rename("equity").to_csv(equity_path, index=True)
    result.weights.to_csv(weights_path, index=True)
    return equity_path, weights_path
