from __future__ import annotations

from dataclasses import dataclass
from datetime import time
from typing import List


import pandas as pd


@dataclass
class FillRecord:
    date: pd.Timestamp
    executed_at: pd.Timestamp
    symbol: str
    action: str
    weight_change: float
    trade_notional: float
    fee_cost: float
    slippage_cost: float
    impact_cost: float
    est_cost: float


def simulate_paper_trades(
    weights: pd.DataFrame,
    init_cash: float = 10000.0,
    execution_time: str = "14:50",
    fee_bps: float = 5.0,
    slippage_bps: float = 5.0,
    amount_df: pd.DataFrame | None = None,
    impact_bps: float = 0.0,
    impact_power: float = 0.5,
    impact_bps_cap_mult: float = 5.0,
) -> List[FillRecord]:
    """
    基于目标权重变化生成“模拟成交记录”。
    不处理订单簿，仅用于纸盘审计和日报展示。
    """
    records: List[FillRecord] = []
    diff = weights.diff().fillna(weights)

    try:
        hh, mm = [int(x) for x in str(execution_time).split(":", 1)]
        exec_t = time(hour=hh, minute=mm)
    except Exception:
        raise ValueError(f"invalid execution_time: {execution_time}")

    in_morning = (time(9, 30) <= exec_t <= time(11, 30))
    in_afternoon = (time(13, 0) <= exec_t <= time(15, 0))
    if not (in_morning or in_afternoon):
        raise ValueError(f"execution_time out of A-share session: {execution_time}")

    fee_rate = fee_bps / 10000.0
    slippage_rate = slippage_bps / 10000.0
    impact_rate_base = max(0.0, impact_bps) / 10000.0
    impact_rate_cap = max(impact_rate_base, impact_rate_base * max(1.0, float(impact_bps_cap_mult)))
    impact_power = max(0.0, float(impact_power))

    if amount_df is not None and not amount_df.empty:
        amount_df = amount_df.reindex(weights.index).fillna(0.0)
    else:
        amount_df = pd.DataFrame(index=weights.index, columns=weights.columns, data=0.0)

    for dt, row in diff.iterrows():
        dt_ts = pd.Timestamp(dt)
        if int(dt_ts.weekday()) >= 5:
            continue

        executed_at = pd.Timestamp.combine(dt_ts.date(), exec_t)
        amount_row = amount_df.loc[dt].reindex(diff.columns).fillna(0.0).astype(float)
        amount_sum = float(amount_row.sum())
        liq_share = (amount_row / amount_sum) if amount_sum > 0 else pd.Series(0.0, index=amount_row.index)

        for symbol, delta in row.items():
            if abs(delta) < 1e-8:
                continue
            action = "BUY" if delta > 0 else "SELL"
            delta_abs = abs(float(delta))
            trade_notional = delta_abs * float(init_cash)
            fee_cost = trade_notional * fee_rate
            slippage_cost = trade_notional * slippage_rate

            if impact_rate_base > 0 and amount_sum > 0:
                ratio = delta_abs / (float(liq_share.get(symbol, 0.0)) + 1e-8)
                impact_rate = min((ratio ** impact_power) * impact_rate_base, impact_rate_cap)
                impact_cost = trade_notional * impact_rate
            else:
                impact_cost = 0.0

            est_cost = fee_cost + slippage_cost + impact_cost
            records.append(
                FillRecord(
                    date=dt_ts,
                    executed_at=executed_at,
                    symbol=str(symbol),
                    action=action,
                    weight_change=float(delta),
                    trade_notional=float(trade_notional),
                    fee_cost=float(fee_cost),
                    slippage_cost=float(slippage_cost),
                    impact_cost=float(impact_cost),
                    est_cost=float(est_cost),
                )
            )
    return records


def fills_to_frame(records: List[FillRecord]) -> pd.DataFrame:
    if not records:
        return pd.DataFrame(columns=["date", "executed_at", "symbol", "action", "weight_change", "trade_notional", "fee_cost", "slippage_cost", "impact_cost", "est_cost"])

    rows = [
        {
            "date": r.date,
            "executed_at": r.executed_at,
            "symbol": r.symbol,
            "action": r.action,
            "weight_change": r.weight_change,
            "trade_notional": r.trade_notional,
            "fee_cost": r.fee_cost,
            "slippage_cost": r.slippage_cost,
            "impact_cost": r.impact_cost,
            "est_cost": r.est_cost,
        }
        for r in records
    ]
    return pd.DataFrame(rows).sort_values(["date", "symbol"]).reset_index(drop=True)
