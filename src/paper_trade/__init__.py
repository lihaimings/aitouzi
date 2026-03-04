from __future__ import annotations

from dataclasses import dataclass
from typing import List


import pandas as pd


@dataclass
class FillRecord:
    date: pd.Timestamp
    symbol: str
    action: str
    weight_change: float
    fee_cost: float
    slippage_cost: float
    impact_cost: float
    est_cost: float


def simulate_paper_trades(
    weights: pd.DataFrame,
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
        amount_row = amount_df.loc[dt].reindex(diff.columns).fillna(0.0).astype(float)
        amount_sum = float(amount_row.sum())
        liq_share = (amount_row / amount_sum) if amount_sum > 0 else pd.Series(0.0, index=amount_row.index)

        for symbol, delta in row.items():
            if abs(delta) < 1e-8:
                continue
            action = "BUY" if delta > 0 else "SELL"
            delta_abs = abs(float(delta))
            fee_cost = delta_abs * fee_rate
            slippage_cost = delta_abs * slippage_rate

            if impact_rate_base > 0 and amount_sum > 0:
                ratio = delta_abs / (float(liq_share.get(symbol, 0.0)) + 1e-8)
                impact_rate = min((ratio ** impact_power) * impact_rate_base, impact_rate_cap)
                impact_cost = delta_abs * impact_rate
            else:
                impact_cost = 0.0

            est_cost = fee_cost + slippage_cost + impact_cost
            records.append(
                FillRecord(
                    date=pd.Timestamp(dt),
                    symbol=str(symbol),
                    action=action,
                    weight_change=float(delta),
                    fee_cost=float(fee_cost),
                    slippage_cost=float(slippage_cost),
                    impact_cost=float(impact_cost),
                    est_cost=float(est_cost),
                )
            )
    return records


def fills_to_frame(records: List[FillRecord]) -> pd.DataFrame:
    if not records:
        return pd.DataFrame(columns=["date", "symbol", "action", "weight_change", "fee_cost", "slippage_cost", "impact_cost", "est_cost"])

    rows = [
        {
            "date": r.date,
            "symbol": r.symbol,
            "action": r.action,
            "weight_change": r.weight_change,
            "fee_cost": r.fee_cost,
            "slippage_cost": r.slippage_cost,
            "impact_cost": r.impact_cost,
            "est_cost": r.est_cost,
        }
        for r in records
    ]
    return pd.DataFrame(rows).sort_values(["date", "symbol"]).reset_index(drop=True)
