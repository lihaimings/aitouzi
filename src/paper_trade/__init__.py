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
    est_cost: float


def simulate_paper_trades(
    weights: pd.DataFrame,
    fee_bps: float = 5.0,
    slippage_bps: float = 5.0,
) -> List[FillRecord]:
    """
    基于目标权重变化生成“模拟成交记录”。
    不处理订单簿，仅用于纸盘审计和日报展示。
    """
    records: List[FillRecord] = []
    diff = weights.diff().fillna(weights)

    per_cost = (fee_bps + slippage_bps) / 10000.0

    for dt, row in diff.iterrows():
        for symbol, delta in row.items():
            if abs(delta) < 1e-8:
                continue
            action = "BUY" if delta > 0 else "SELL"
            est_cost = abs(float(delta)) * per_cost
            records.append(
                FillRecord(
                    date=pd.Timestamp(dt),
                    symbol=str(symbol),
                    action=action,
                    weight_change=float(delta),
                    est_cost=float(est_cost),
                )
            )
    return records


def fills_to_frame(records: List[FillRecord]) -> pd.DataFrame:
    if not records:
        return pd.DataFrame(columns=["date", "symbol", "action", "weight_change", "est_cost"])

    rows = [
        {
            "date": r.date,
            "symbol": r.symbol,
            "action": r.action,
            "weight_change": r.weight_change,
            "est_cost": r.est_cost,
        }
        for r in records
    ]
    return pd.DataFrame(rows).sort_values(["date", "symbol"]).reset_index(drop=True)
