from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Dict, List

import akshare as ak
import baostock as bs
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data" / "minute"
STATE_DIR = ROOT / "state"
REPORTS_DIR = ROOT / "reports"
DATA_DIR.mkdir(parents=True, exist_ok=True)
STATE_DIR.mkdir(parents=True, exist_ok=True)
REPORTS_DIR.mkdir(parents=True, exist_ok=True)

EM_URL = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
EM_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://quote.eastmoney.com/",
}
TENCENT_MIN_URL = "https://ifzq.gtimg.cn/appstock/app/kline/mkline"


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
    return [f"1.{code}", f"0.{code}"]


def _load_codes(layer: str = "l2") -> List[str]:
    p = ROOT / "data" / "layers" / ("l2_core_codes.csv" if layer == "l2" else "l1_watch_codes.csv")
    if not p.exists():
        return []
    df = pd.read_csv(p, dtype={"code": str})
    if "code" not in df.columns:
        return []
    return [str(x).zfill(6) for x in df["code"].dropna().tolist()]


def _period_to_klt(period: str) -> str:
    mapping = {"1": "1", "5": "5", "15": "15", "30": "30", "60": "60"}
    return mapping.get(str(period), "15")


def _to_bs_code(code: str) -> str:
    code = str(code).zfill(6)
    return f"sz.{code}" if code.startswith(("0", "1", "2", "3")) else f"sh.{code}"


def _parse_em_klines(klines: list[str]) -> pd.DataFrame:
    rows = []
    for line in klines:
        parts = str(line).split(",")
        if len(parts) < 7:
            continue
        rows.append(
            {
                "datetime": parts[0],
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
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    for c in ["open", "high", "low", "close", "volume", "amount"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df.dropna(subset=["datetime", "close"]).sort_values("datetime").reset_index(drop=True)


def _to_tencent_code(code: str) -> str:
    code = str(code).zfill(6)
    return ("sh" + code) if code.startswith(("5", "6", "9")) else ("sz" + code)


def _period_to_tencent(period: str) -> str:
    m = {"1": "m1", "5": "m5", "15": "m15", "30": "m30", "60": "m60"}
    return m.get(str(period), "m15")


def fetch_one_tencent(code: str, period: str = "15", timeout: int = 20, limit: int = 12000) -> pd.DataFrame:
    tcode = _to_tencent_code(code)
    cyc = _period_to_tencent(period)
    params = {"param": f"{tcode},{cyc},,{int(limit)}"}
    r = _build_session().get(TENCENT_MIN_URL, params=params, timeout=timeout)
    r.raise_for_status()
    data = r.json().get("data", {}).get(tcode, {})
    rows = data.get(cyc) or []
    out = []
    for x in rows:
        # [YYYYmmddHHMM, open, close, high, low, volume, {}, amount]
        if len(x) < 6:
            continue
        dt = pd.to_datetime(str(x[0]), format="%Y%m%d%H%M", errors="coerce")
        if pd.isna(dt):
            continue
        out.append(
            {
                "datetime": dt,
                "open": pd.to_numeric(x[1], errors="coerce"),
                "close": pd.to_numeric(x[2], errors="coerce"),
                "high": pd.to_numeric(x[3], errors="coerce"),
                "low": pd.to_numeric(x[4], errors="coerce"),
                "volume": pd.to_numeric(x[5], errors="coerce"),
                "amount": pd.to_numeric(x[7], errors="coerce") if len(x) > 7 else pd.NA,
            }
        )
    df = pd.DataFrame(out)
    if df.empty:
        raise RuntimeError(f"tencent minute empty for {code}")
    df = df.dropna(subset=["datetime", "close"]).sort_values("datetime").drop_duplicates("datetime", keep="last").reset_index(drop=True)
    return df


def fetch_one_em(code: str, period: str = "15", timeout: int = 20) -> pd.DataFrame:
    klt = _period_to_klt(period)
    session = _build_session()
    last_error = None
    for secid in _secids(code):
        try:
            params = {
                "secid": secid,
                "klt": klt,
                "fqt": "1",
                "beg": "0",
                "end": "20500101",
                "fields1": "f1,f2,f3,f4,f5,f6",
                "fields2": "f51,f52,f53,f54,f55,f56,f57,f58",
            }
            resp = session.get(EM_URL, params=params, headers=EM_HEADERS, timeout=timeout)
            resp.raise_for_status()
            data = resp.json()
            klines = ((data.get("data") or {}).get("klines") or [])
            if not klines:
                continue
            df = _parse_em_klines(klines)
            if not df.empty:
                return df
        except Exception as e:
            last_error = e
    raise RuntimeError(f"em minute failed for {code}: {last_error}")


def fetch_one_ak(code: str, period: str = "15") -> pd.DataFrame:
    df = ak.fund_etf_hist_min_em(symbol=code, period=period, adjust="")
    df = df.rename(
        columns={
            "时间": "datetime",
            "开盘": "open",
            "收盘": "close",
            "最高": "high",
            "最低": "low",
            "成交量": "volume",
            "成交额": "amount",
        }
    )
    if df.empty:
        return df
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    for c in ["open", "high", "low", "close", "volume", "amount"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df.dropna(subset=["datetime", "close"]).sort_values("datetime").reset_index(drop=True)


def fetch_one_bs(code: str, period: str = "15") -> pd.DataFrame:
    lg = bs.login()
    if lg.error_code != "0":
        raise RuntimeError(f"baostock login failed: {lg.error_msg}")
    try:
        bs_code = _to_bs_code(code)
        rs = bs.query_history_k_data_plus(
            bs_code,
            "date,time,open,high,low,close,volume,amount",
            start_date="2010-01-01",
            end_date="2030-12-31",
            frequency=str(period),
            adjustflag="2",
        )
        if rs.error_code != "0":
            raise RuntimeError(f"baostock query failed: {rs.error_msg}")
        rows = []
        while rs.next():
            rows.append(rs.get_row_data())
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows, columns=rs.fields)
        # bs time like 20260331100000000
        df["datetime"] = pd.to_datetime(df["time"].astype(str).str[:14], format="%Y%m%d%H%M%S", errors="coerce")
        for c in ["open", "high", "low", "close", "volume", "amount"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        out = df[["datetime", "open", "high", "low", "close", "volume", "amount"]].dropna(subset=["datetime", "close"])
        return out.sort_values("datetime").reset_index(drop=True)
    finally:
        try:
            bs.logout()
        except Exception:
            pass


def _read_done(state_path: Path) -> Dict[str, dict]:
    if not state_path.exists():
        return {}
    try:
        return json.loads(state_path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _write_done(state_path: Path, obj: Dict[str, dict]) -> None:
    state_path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Fetch ETF minute cache for backtest (crawler-first)")
    ap.add_argument("--layer", choices=["l1", "l2"], default="l2")
    ap.add_argument("--period", default="15")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--resume", type=int, default=1)
    ap.add_argument("--max-retries", type=int, default=3)
    ap.add_argument("--base-sleep", type=float, default=0.8)
    args = ap.parse_args()

    codes = _load_codes(args.layer)
    if args.limit > 0:
        codes = codes[: args.limit]

    state_path = STATE_DIR / f"minute_fetch_state_{args.layer}_{args.period}m.json"
    done = _read_done(state_path) if int(args.resume) == 1 else {}

    ok, fail = 0, 0
    failed_codes = []

    for code in codes:
        if code in done and done[code].get("status") == "ok":
            continue

        success = False
        err_msg = ""
        source_used = ""

        for i in range(max(1, int(args.max_retries))):
            try:
                # crawler-first: tencent ifzq minute endpoint (works in current env)
                df = fetch_one_tencent(code, period=args.period)
                if df.empty:
                    raise RuntimeError("empty from tencent")
                source_used = "tencent-ifzq"
                success = True
                break
            except Exception as e0:
                try:
                    # fallback1: eastmoney web endpoint
                    df = fetch_one_em(code, period=args.period)
                    if df.empty:
                        raise RuntimeError("empty from em")
                    source_used = "eastmoney-web"
                    success = True
                    break
                except Exception as e1:
                    try:
                        # fallback2: akshare wrapper
                        df = fetch_one_ak(code, period=args.period)
                        if df.empty:
                            raise RuntimeError("empty from ak")
                        source_used = "akshare-fallback"
                        success = True
                        break
                    except Exception as e2:
                        try:
                            # fallback3: baostock minute history
                            df = fetch_one_bs(code, period=args.period)
                            if df.empty:
                                raise RuntimeError("empty from baostock")
                            source_used = "baostock-fallback"
                            success = True
                            break
                        except Exception as e3:
                            err_msg = f"{e0} | {e1} | {e2} | {e3}"
                            time.sleep(float(args.base_sleep) * (2 ** i))

        if success:
            out = DATA_DIR / f"etf_{code}_{args.period}m.csv"
            df.to_csv(out, index=False)
            ok += 1
            done[code] = {"status": "ok", "rows": int(len(df)), "source": source_used, "updated_at": pd.Timestamp.now().isoformat()}
            print(f"[ok] {code} rows={len(df)} source={source_used} -> {out}")
        else:
            fail += 1
            failed_codes.append(code)
            done[code] = {"status": "fail", "error": err_msg[:240], "updated_at": pd.Timestamp.now().isoformat()}
            print(f"[fail] {code}: {err_msg[:200]}")

        _write_done(state_path, done)
        time.sleep(float(args.base_sleep))

    report = {
        "layer": args.layer,
        "period": args.period,
        "total": len(codes),
        "ok": ok,
        "fail": fail,
        "failed_codes": failed_codes,
        "updated_at": pd.Timestamp.now().isoformat(),
        "state_path": str(state_path),
    }
    report_path = REPORTS_DIR / f"minute_fetch_report_{args.layer}_{args.period}m.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"done: ok={ok}, fail={fail}, total={len(codes)}")
    print(f"state: {state_path}")
    print(f"report: {report_path}")
    return 0 if fail == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
