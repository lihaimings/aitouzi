from __future__ import annotations

import json
from typing import Optional

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

THS_DAILY_URL = "http://d.10jqka.com.cn/v6/line/hs_{code}/01/last36000.js"
THS_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36",
    "Referer": "http://www.iwencai.com/",
}


def _build_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s


def _extract_json(text: str) -> dict:
    left = text.find("{")
    right = text.rfind("}")
    if left < 0 or right < 0 or right <= left:
        return {}
    return json.loads(text[left : right + 1])


def _parse_klines(raw: str, beg: Optional[str], end: Optional[str]) -> pd.DataFrame:
    rows = []
    for item in str(raw or "").split(";"):
        if not item:
            continue
        parts = item.split(",")
        if len(parts) < 7:
            continue
        rows.append(
            {
                "date": parts[0],
                "open": parts[1],
                "high": parts[2],
                "low": parts[3],
                "close": parts[4],
                "volume": parts[5],
                "amount": parts[6],
            }
        )

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df["date"] = pd.to_datetime(df["date"], format="%Y%m%d", errors="coerce")
    for col in ["open", "high", "low", "close", "volume", "amount"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["date", "close"]).sort_values("date").reset_index(drop=True)
    if beg:
        beg_ts = pd.to_datetime(beg, format="%Y%m%d", errors="coerce")
        if pd.notna(beg_ts):
            df = df[df["date"] >= beg_ts]
    if end:
        end_ts = pd.to_datetime(end, format="%Y%m%d", errors="coerce")
        if pd.notna(end_ts):
            df = df[df["date"] <= end_ts]
    return df.reset_index(drop=True)


def fetch_etf_daily(code: str, beg: Optional[str] = None, end: Optional[str] = None, timeout: int = 20) -> pd.DataFrame:
    code = str(code).strip()
    if not (len(code) == 6 and code.isdigit()):
        raise RuntimeError(f"invalid etf code: {code}")

    session = _build_session()
    url = THS_DAILY_URL.format(code=code)
    try:
        resp = session.get(url, headers=THS_HEADERS, timeout=timeout)
        resp.raise_for_status()
        payload = _extract_json(resp.text)
        key = f"hs_{code}"
        node = payload.get(key) or {}
        raw = node.get("data", "")
        df = _parse_klines(raw, beg=beg, end=end)
        if df.empty:
            raise RuntimeError("ths empty data")
        return df
    except Exception as e:
        raise RuntimeError(f"ths fetch failed for {code}: {e}")
