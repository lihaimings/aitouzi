from __future__ import annotations

import argparse
import json
import os
from datetime import datetime
from pathlib import Path
from typing import List

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data" / "minute"
STATE_DIR = ROOT / "state"
REPORTS_DIR = ROOT / "reports"


def to_jq_code(code: str) -> str:
    code = str(code).zfill(6)
    return f"{code}.XSHG" if code.startswith(("5", "6", "9")) else f"{code}.XSHE"


def load_codes(layer: str = "l1", limit: int = 0) -> List[str]:
    p = ROOT / "data" / "layers" / ("l1_watch_codes.csv" if layer == "l1" else "l2_core_codes.csv")
    if not p.exists():
        return []
    d = pd.read_csv(p, dtype={"code": str})
    codes = [str(x).zfill(6) for x in d.get("code", []).dropna().tolist()]
    if limit > 0:
        codes = codes[:limit]
    return codes


def main() -> int:
    ap = argparse.ArgumentParser(description="Fetch real 15m ETF history from JoinQuant")
    ap.add_argument("--layer", choices=["l1", "l2"], default="l1")
    ap.add_argument("--codes", nargs="*", default=None)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--count", type=int, default=60000, help="bar count for get_price")
    ap.add_argument("--username", default=os.getenv("JQ_USERNAME", ""))
    ap.add_argument("--password", default=os.getenv("JQ_PASSWORD", ""))
    args = ap.parse_args()

    if not args.username or not args.password:
        raise SystemExit("missing JoinQuant credentials: set JQ_USERNAME/JQ_PASSWORD or pass --username/--password")

    import jqdatasdk as jq

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    jq.auth(args.username, args.password)

    codes = [str(c).zfill(6) for c in args.codes] if args.codes else load_codes(args.layer, args.limit)
    if not codes:
        raise SystemExit("no codes to fetch")

    status = {}
    ok = fail = 0
    for code in codes:
        jq_code = to_jq_code(code)
        try:
            df = jq.get_price(
                jq_code,
                count=int(args.count),
                frequency="15m",
                fields=["open", "high", "low", "close", "volume", "money"],
                skip_paused=False,
                fq="pre",
                panel=False,
            )
            if df is None or len(df) == 0:
                raise RuntimeError("empty dataframe")

            if "time" in df.columns:
                dt_col = "time"
            elif "datetime" in df.columns:
                dt_col = "datetime"
            else:
                dt_col = df.columns[0]

            out = pd.DataFrame(
                {
                    "datetime": pd.to_datetime(df[dt_col], errors="coerce"),
                    "open": pd.to_numeric(df["open"], errors="coerce"),
                    "high": pd.to_numeric(df["high"], errors="coerce"),
                    "low": pd.to_numeric(df["low"], errors="coerce"),
                    "close": pd.to_numeric(df["close"], errors="coerce"),
                    "volume": pd.to_numeric(df["volume"], errors="coerce"),
                    "amount": pd.to_numeric(df["money"], errors="coerce"),
                }
            ).dropna(subset=["datetime", "close"]).sort_values("datetime")

            out_path = DATA_DIR / f"etf_{code}_15m.csv"
            out.to_csv(out_path, index=False)
            ok += 1
            status[code] = {
                "ok": True,
                "rows": int(len(out)),
                "min_datetime": str(out["datetime"].min()),
                "max_datetime": str(out["datetime"].max()),
                "source": "joinquant",
                "path": str(out_path),
            }
            print(f"[ok] {code} rows={len(out)}")
        except Exception as e:
            fail += 1
            status[code] = {"ok": False, "error": str(e)}
            print(f"[fail] {code}: {e}")

    report = {
        "updated_at": datetime.now().isoformat(timespec="seconds"),
        "layer": args.layer,
        "total": len(codes),
        "ok": ok,
        "fail": fail,
        "status": status,
    }

    report_path = REPORTS_DIR / f"minute_fetch_joinquant_{args.layer}_15m.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    state_path = STATE_DIR / f"minute_fetch_joinquant_{args.layer}_15m.json"
    state_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"done: ok={ok}, fail={fail}, total={len(codes)}")
    print(f"report: {report_path}")
    return 0 if fail == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
