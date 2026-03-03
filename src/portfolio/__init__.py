from __future__ import annotations

import pandas as pd


def cap_weight_and_normalize(
    weights: pd.DataFrame,
    max_single_weight: float = 0.6,
) -> pd.DataFrame:
    """单资产权重上限约束，并将每期权重归一化到 <=1。"""
    w = weights.clip(lower=0.0, upper=max_single_weight).copy()
    row_sum = w.sum(axis=1)
    row_sum = row_sum.where(row_sum > 1.0, 1.0)
    return w.div(row_sum, axis=0).fillna(0.0)
