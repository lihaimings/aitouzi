import pandas as pd
import numpy as np
from pathlib import Path

WINDOWS = {
    'mom20': 20,
    'mom60': 60,
}


def compute_momentum(df: pd.DataFrame, window: int) -> pd.Series:
    return df['close'].pct_change(window)


def signal_from_prices(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame(index=df['date'])
    for name, w in WINDOWS.items():
        out[name] = compute_momentum(df, w)
    out['score'] = out.rank(pct=True).mean(axis=1)
    return out
