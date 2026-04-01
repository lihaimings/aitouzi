from __future__ import annotations

from typing import Optional

import pandas as pd
import akshare as ak


def _to_symbol(code: str) -> str:
    c = str(code).strip()
    if c.startswith(("5", "6")):
        return f"sh{c}"
    return f"sz{c}"


def fetch_etf_daily(code: str, beg: Optional[str] = None, end: Optional[str] = None) -> pd.DataFrame:
    symbol = _to_symbol(code)
    start = str(beg or "20100101")
    finish = str(end or "20500101")

    df = ak.stock_zh_a_hist_tx(symbol=symbol, start_date=start, end_date=finish, adjust="")
    if df is None or df.empty:
        raise RuntimeError("tencent empty data")

    # expected cols: date/open/close/high/low/amount
    rename = {
        "date": "date",
        "open": "open",
        "high": "high",
        "low": "low",
        "close": "close",
        "amount": "amount",
        "volume": "volume",
    }
    df = df.rename(columns=rename)

    for col in ["open", "high", "low", "close", "volume", "amount"]:
        if col not in df.columns:
            df[col] = pd.NA
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "close"]).sort_values("date").reset_index(drop=True)
    if df.empty:
        raise RuntimeError("tencent normalized empty data")
    return df
