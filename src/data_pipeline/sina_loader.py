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
    df = ak.fund_etf_hist_sina(symbol=symbol)
    if df is None or df.empty:
        raise RuntimeError("sina empty data")

    rename = {
        "date": "date",
        "open": "open",
        "high": "high",
        "low": "low",
        "close": "close",
        "volume": "volume",
    }
    df = df.rename(columns=rename)
    for col in ["open", "high", "low", "close", "volume", "amount"]:
        if col not in df.columns:
            df[col] = pd.NA
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    if beg:
        b = pd.to_datetime(beg, errors="coerce")
        if pd.notna(b):
            df = df[df["date"] >= b]
    if end:
        e = pd.to_datetime(end, errors="coerce")
        if pd.notna(e):
            df = df[df["date"] <= e]

    df = df.dropna(subset=["date", "close"]).sort_values("date").reset_index(drop=True)
    if df.empty:
        raise RuntimeError("sina normalized empty data")
    return df
