from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

DATA_DIR = Path(__file__).resolve().parents[2] / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

EM_URL = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
EM_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36",
    "Referer": "https://quote.eastmoney.com/",
}


def _build_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=0.4,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s


def _secids(code: str) -> list[str]:
    code = str(code).strip()
    if code.startswith("159"):
        return [f"0.{code}", f"1.{code}"]
    if code.startswith(("510", "511", "512", "513", "515", "516", "517", "518", "56", "58")):
        return [f"1.{code}", f"0.{code}"]
    return [f"1.{code}", f"0.{code}"]


def _parse_klines(klines: list[str]) -> pd.DataFrame:
    rows = []
    for line in klines:
        parts = str(line).split(",")
        if len(parts) < 7:
            continue
        rows.append(
            {
                "date": parts[0],
                "open": parts[1],
                "close": parts[2],
                "high": parts[3],
                "low": parts[4],
                "volume": parts[5],
                "amount": parts[6],
            }
        )

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    for col in ["open", "high", "low", "close", "volume", "amount"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.dropna(subset=["date", "close"]).sort_values("date").reset_index(drop=True)


def fetch_etf_daily(code: str, beg: Optional[str] = None, end: Optional[str] = None, timeout: int = 20) -> pd.DataFrame:
    beg = beg or "20100101"
    end = end or "20500101"

    last_error = None
    session = _build_session()
    for secid in _secids(code):
        try:
            params = {
                "secid": secid,
                "klt": "101",
                "fqt": "1",
                "beg": beg,
                "end": end,
                "fields1": "f1,f2,f3,f4,f5,f6",
                "fields2": "f51,f52,f53,f54,f55,f56,f57,f58",
            }
            resp = session.get(EM_URL, params=params, headers=EM_HEADERS, timeout=timeout)
            resp.raise_for_status()
            data = resp.json()
            klines = ((data.get("data") or {}).get("klines") or [])
            if not klines:
                continue
            df = _parse_klines(klines)
            if not df.empty:
                return df
        except Exception as e:
            last_error = e
            continue

    raise RuntimeError(f"eastmoney fetch failed for {code}: {last_error}")


def cache_etf_daily(code: str, beg: Optional[str] = None, end: Optional[str] = None) -> Path:
    df = fetch_etf_daily(code=code, beg=beg, end=end)
    out = DATA_DIR / f"etf_{code}_eastmoney.csv"
    df.to_csv(out, index=False)
    return out
