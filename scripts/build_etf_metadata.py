from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
OUT_PATH = DATA_DIR / "etf_metadata.csv"

QUOTE_URL = "https://push2.eastmoney.com/api/qt/stock/get"
HEADERS = {
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


def _secids(code: str) -> List[str]:
    code = str(code).strip()
    if code.startswith("159"):
        return [f"0.{code}", f"1.{code}"]
    return [f"1.{code}", f"0.{code}"]


def _parse_ymd(v) -> Optional[pd.Timestamp]:
    s = str(v).strip()
    if not s or s in {"None", "nan", "NaT", "0"}:
        return None
    if s.isdigit() and len(s) == 8:
        dt = pd.to_datetime(s, format="%Y%m%d", errors="coerce")
    else:
        dt = pd.to_datetime(s, errors="coerce")
    if pd.isna(dt):
        return None
    return pd.Timestamp(dt)


def _fetch_quote_meta(session: requests.Session, code: str) -> Dict:
    fields = "f57,f58,f26,f189"
    last_error = None
    for secid in _secids(code):
        try:
            resp = session.get(QUOTE_URL, params={"secid": secid, "fields": fields}, headers=HEADERS, timeout=20)
            resp.raise_for_status()
            data = (resp.json() or {}).get("data") or {}
            if not data:
                continue
            list_dt = _parse_ymd(data.get("f189")) or _parse_ymd(data.get("f26"))
            return {
                "meta_name": str(data.get("f58") or ""),
                "listed_date": str(list_dt.date()) if list_dt is not None else "",
                "meta_source": "eastmoney_quote",
            }
        except Exception as e:
            last_error = e
            continue
    return {
        "meta_name": "",
        "listed_date": "",
        "meta_source": f"fallback_cache_only:{str(last_error)[:80]}" if last_error else "fallback_cache_only",
    }


def _best_local_cache(code: str) -> Optional[Path]:
    candidates = sorted(DATA_DIR.glob(f"etf_{code}*.csv"))
    best_path = None
    best_rows = -1
    for p in candidates:
        try:
            n = len(pd.read_csv(p, usecols=["date"]))
        except Exception:
            continue
        if n > best_rows:
            best_rows = n
            best_path = p
    return best_path


def _cache_stats(code: str) -> Dict:
    p = _best_local_cache(code)
    if p is None:
        return {
            "cache_file": "",
            "cache_rows": 0,
            "cache_start": "",
            "cache_end": "",
            "cache_years": 0.0,
        }
    try:
        d = pd.read_csv(p, usecols=["date"]) 
        dt = pd.to_datetime(d["date"], errors="coerce").dropna()
        if dt.empty:
            return {
                "cache_file": str(p),
                "cache_rows": 0,
                "cache_start": "",
                "cache_end": "",
                "cache_years": 0.0,
            }
        start = pd.Timestamp(dt.min())
        end = pd.Timestamp(dt.max())
        years = float((end - start).days / 365.25)
        return {
            "cache_file": str(p),
            "cache_rows": int(len(dt)),
            "cache_start": str(start.date()),
            "cache_end": str(end.date()),
            "cache_years": round(years, 3),
        }
    except Exception:
        return {
            "cache_file": str(p),
            "cache_rows": 0,
            "cache_start": "",
            "cache_end": "",
            "cache_years": 0.0,
        }


def _load_codes(scope: str) -> List[str]:
    if scope == "universe" and (DATA_DIR / "etf_universe.csv").exists():
        u = pd.read_csv(DATA_DIR / "etf_universe.csv", dtype={"code": str})
        codes = [str(x).zfill(6) for x in u.get("code", []).tolist()]
        return sorted(set([c for c in codes if len(c) == 6 and c.isdigit()]))

    found = set()
    for p in DATA_DIR.glob("etf_*.csv"):
        m = re.match(r"etf_(\d{6})(?:_.*)?\.csv$", p.name)
        if m:
            found.add(m.group(1))
    return sorted(found)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build ETF metadata (listed date + cache depth)")
    parser.add_argument("--scope", choices=["universe", "all"], default="universe")
    args = parser.parse_args()

    codes = _load_codes(scope=args.scope)
    if not codes:
        print("no codes found")
        return 1

    now = pd.Timestamp.today().normalize()
    session = _build_session()
    rows = []
    for i, code in enumerate(codes, start=1):
        meta = _fetch_quote_meta(session=session, code=code)
        cache = _cache_stats(code=code)
        listed_ts = _parse_ymd(meta.get("listed_date"))
        listed_years = float((now - listed_ts).days / 365.25) if listed_ts is not None else float("nan")
        history_gap_years = float(listed_years - float(cache.get("cache_years", 0.0))) if pd.notna(listed_years) else float("nan")
        likely_shortfall = bool(pd.notna(history_gap_years) and history_gap_years > 1.0 and int(cache.get("cache_rows", 0)) < 1200)

        rows.append(
            {
                "code": code,
                "name": meta.get("meta_name", ""),
                "listed_date": meta.get("listed_date", ""),
                "listed_years": round(float(listed_years), 3) if pd.notna(listed_years) else "",
                "cache_rows": int(cache.get("cache_rows", 0)),
                "cache_start": cache.get("cache_start", ""),
                "cache_end": cache.get("cache_end", ""),
                "cache_years": float(cache.get("cache_years", 0.0)),
                "history_gap_years": round(float(history_gap_years), 3) if pd.notna(history_gap_years) else "",
                "likely_history_shortfall": likely_shortfall,
                "meta_source": meta.get("meta_source", ""),
                "cache_file": cache.get("cache_file", ""),
                "updated_at": pd.Timestamp.now().isoformat(),
            }
        )
        if i % 20 == 0:
            print(f"processed {i}/{len(codes)}")

    df = pd.DataFrame(rows)
    df.to_csv(OUT_PATH, index=False, encoding="utf-8-sig")
    print(f"saved metadata: {OUT_PATH}")
    print(f"codes: {len(df)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
