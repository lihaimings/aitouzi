from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd

DATA_DIR = Path(__file__).resolve().parents[2] / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)


def _normalize_ef_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(
        columns={
            "日期": "date",
            "开盘": "open",
            "收盘": "close",
            "最高": "high",
            "最低": "low",
            "成交量": "volume",
            "成交额": "amount",
        }
    )
    for col in ["open", "high", "low", "close", "volume", "amount"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df.dropna(subset=["date", "close"]).sort_values("date").reset_index(drop=True)


def fetch_etf_daily(code: str, beg: Optional[str] = None, end: Optional[str] = None) -> pd.DataFrame:
    try:
        import efinance as ef  # type: ignore
    except Exception as e:
        raise RuntimeError(f"efinance not available: {e}")

    kwargs = {"klt": 101}
    if beg:
        kwargs["beg"] = beg
    if end:
        kwargs["end"] = end

    df = ef.stock.get_quote_history(code, **kwargs)
    if df is None or len(df) == 0:
        raise RuntimeError("efinance returned empty dataframe")
    return _normalize_ef_df(df)


def cache_etf_daily(code: str, beg: Optional[str] = None, end: Optional[str] = None) -> Path:
    df = fetch_etf_daily(code, beg=beg, end=end)
    out = DATA_DIR / f"etf_{code}_efinance.csv"
    df.to_csv(out, index=False)
    return out
