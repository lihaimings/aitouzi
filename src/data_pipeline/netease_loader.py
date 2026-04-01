from __future__ import annotations

from io import StringIO
from typing import Optional

import pandas as pd
import requests


def _code(code: str) -> str:
    c = str(code).strip()
    return ("0" + c) if c.startswith(("5", "6")) else ("1" + c)


def fetch_etf_daily(code: str, beg: Optional[str] = None, end: Optional[str] = None, timeout: int = 20) -> pd.DataFrame:
    start = str(beg or "20100101")
    finish = str(end or pd.Timestamp.today().strftime("%Y%m%d"))
    url = "https://quotes.money.163.com/service/chddata.html"
    params = {
        "code": _code(code),
        "start": start,
        "end": finish,
        "fields": "TOPEN;HIGH;LOW;TCLOSE;VOTURNOVER;VATURNOVER",
    }
    r = requests.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    txt = r.content.decode("gbk", errors="ignore")
    if "日期" not in txt:
        raise RuntimeError("netease empty data")

    df = pd.read_csv(StringIO(txt))
    rename = {
        "日期": "date",
        "开盘价": "open",
        "最高价": "high",
        "最低价": "low",
        "收盘价": "close",
        "成交量": "volume",
        "成交金额": "amount",
    }
    df = df.rename(columns=rename)
    for col in ["open", "high", "low", "close", "volume", "amount"]:
        if col not in df.columns:
            df[col] = pd.NA
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "close"]).sort_values("date").reset_index(drop=True)
    if df.empty:
        raise RuntimeError("netease normalized empty data")
    return df
