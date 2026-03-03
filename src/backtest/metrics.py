from __future__ import annotations

from typing import Dict, Optional

import numpy as np
import pandas as pd


def compute_performance_metrics(
    equity: pd.Series,
    daily_returns: pd.Series,
    benchmark_returns: Optional[pd.Series] = None,
    trading_days: int = 252,
) -> Dict[str, float]:
    equity = pd.Series(equity).dropna()
    daily_returns = pd.Series(daily_returns).fillna(0.0)

    if len(equity) < 3:
        base = {
            "total_return": 0.0,
            "annual_return": 0.0,
            "annual_vol": 0.0,
            "sharpe": 0.0,
            "sortino": 0.0,
            "max_drawdown": 0.0,
            "calmar": 0.0,
            "win_rate": 0.0,
        }
        if benchmark_returns is not None:
            base["alpha_annual"] = 0.0
        return base

    total_return = float(equity.iloc[-1] / equity.iloc[0] - 1.0)
    n_days = len(equity)
    annual_return = float((1.0 + total_return) ** (trading_days / max(n_days, 1)) - 1.0)

    annual_vol = float(daily_returns.std(ddof=0) * np.sqrt(trading_days))
    sharpe = float(annual_return / annual_vol) if annual_vol > 0 else 0.0

    downside = daily_returns.clip(upper=0.0)
    downside_vol = float(downside.std(ddof=0) * np.sqrt(trading_days))
    sortino = float(annual_return / downside_vol) if downside_vol > 0 else 0.0

    rolling_max = equity.cummax()
    drawdown = equity / rolling_max - 1.0
    max_drawdown = float(drawdown.min())
    calmar = float(annual_return / abs(max_drawdown)) if max_drawdown < 0 else 0.0

    win_rate = float((daily_returns > 0).mean())

    metrics = {
        "total_return": total_return,
        "annual_return": annual_return,
        "annual_vol": annual_vol,
        "sharpe": sharpe,
        "sortino": sortino,
        "max_drawdown": max_drawdown,
        "calmar": calmar,
        "win_rate": win_rate,
    }

    if benchmark_returns is not None:
        bench = pd.Series(benchmark_returns).reindex(daily_returns.index).fillna(0.0)
        active = daily_returns - bench
        alpha_annual = float(active.mean() * trading_days)
        metrics["alpha_annual"] = alpha_annual

    return metrics
