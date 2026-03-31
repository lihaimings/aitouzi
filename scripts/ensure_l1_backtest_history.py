from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
REPORTS_DIR = ROOT / "reports"
STATE_DIR = ROOT / "state"


def _load_l1_codes() -> List[str]:
    p = DATA_DIR / "layers" / "l1_watch_codes.csv"
    if not p.exists():
        return []
    df = pd.read_csv(p, dtype={"code": str})
    return sorted({str(x).zfill(6) for x in df.get("code", []).dropna().tolist() if str(x).strip()})


def _load_meta() -> pd.DataFrame:
    p = DATA_DIR / "etf_metadata.csv"
    if not p.exists():
        return pd.DataFrame(columns=["code", "listed_date", "listed_years"])
    df = pd.read_csv(p, dtype={"code": str})
    df["code"] = df["code"].map(lambda x: str(x).zfill(6))
    if "listed_date" not in df.columns:
        df["listed_date"] = pd.NaT
    df["listed_date"] = pd.to_datetime(df["listed_date"], errors="coerce")
    if "listed_years" not in df.columns:
        df["listed_years"] = pd.NA
    df["listed_years"] = pd.to_numeric(df["listed_years"], errors="coerce")
    keep = [c for c in ["code", "name", "listed_date", "listed_years"] if c in df.columns]
    return df[keep].drop_duplicates("code", keep="last")


def _cache_stats(code: str) -> Dict:
    p = DATA_DIR / f"etf_{code}.csv"
    if not p.exists():
        return {"exists": False, "rows": 0, "cache_start": None, "cache_end": None}
    try:
        d = pd.read_csv(p, usecols=["date"])
        dt = pd.to_datetime(d["date"], errors="coerce").dropna()
        if dt.empty:
            return {"exists": True, "rows": 0, "cache_start": None, "cache_end": None}
        return {
            "exists": True,
            "rows": int(len(dt)),
            "cache_start": pd.Timestamp(dt.min()),
            "cache_end": pd.Timestamp(dt.max()),
        }
    except Exception:
        return {"exists": True, "rows": 0, "cache_start": None, "cache_end": None}


def _run(cmd: List[str]) -> None:
    r = subprocess.run(cmd, cwd=str(ROOT), capture_output=True, text=True)
    if r.stdout:
        print(r.stdout)
    if r.stderr:
        print(r.stderr)
    if r.returncode != 0:
        raise RuntimeError(f"command failed: {' '.join(cmd)}")


def main() -> int:
    ap = argparse.ArgumentParser(description="Ensure L1 has backtest history: min(3y, listed_age)")
    ap.add_argument("--max-years", type=float, default=3.0)
    ap.add_argument("--recent-days", type=int, default=3)
    ap.add_argument("--start-tolerance-days", type=int, default=7)
    ap.add_argument("--repair", action="store_true")
    ap.add_argument("--batch-size", type=int, default=20)
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    STATE_DIR.mkdir(parents=True, exist_ok=True)

    codes = _load_l1_codes()
    if args.limit > 0:
        codes = codes[: args.limit]
    if not codes:
        print("no L1 codes found")
        return 1

    meta = _load_meta()
    now = pd.Timestamp(datetime.now().date())
    cutoff = now - pd.Timedelta(days=int(args.recent_days))

    rows = []
    need_codes = []

    for code in codes:
        m = meta[meta["code"] == code]
        listed_date = pd.NaT if m.empty else m.iloc[0].get("listed_date")
        if pd.isna(listed_date):
            listed_date = now - pd.Timedelta(days=int(args.max_years * 365.25))
        target_start = max(listed_date, now - pd.Timedelta(days=int(args.max_years * 365.25)))

        st = _cache_stats(code)
        cache_start = st["cache_start"]
        cache_end = st["cache_end"]

        start_ok = (cache_start is not None) and (cache_start <= target_start + pd.Timedelta(days=int(args.start_tolerance_days)))
        end_ok = (cache_end is not None) and (cache_end >= cutoff)
        ok = bool(start_ok and end_ok and st["rows"] > 0)

        if not ok:
            need_codes.append(code)

        rows.append(
            {
                "code": code,
                "listed_date": str(listed_date.date()) if not pd.isna(listed_date) else None,
                "target_start": str(target_start.date()),
                "cache_start": str(cache_start.date()) if cache_start is not None else None,
                "cache_end": str(cache_end.date()) if cache_end is not None else None,
                "rows": int(st["rows"]),
                "start_ok": bool(start_ok),
                "end_ok": bool(end_ok),
                "ok": ok,
            }
        )

    out_df = pd.DataFrame(rows).sort_values(["ok", "rows"], ascending=[True, True])
    report_csv = REPORTS_DIR / "l1_backtest_history_audit.csv"
    out_df.to_csv(report_csv, index=False, encoding="utf-8-sig")

    summary = {
        "updated_at": datetime.now().isoformat(timespec="seconds"),
        "l1_total": len(codes),
        "ok_count": int(out_df["ok"].sum()),
        "need_repair_count": len(need_codes),
        "need_repair_codes": need_codes,
        "report_csv": str(report_csv),
    }

    state_path = STATE_DIR / "l1_backtest_history_status.json"
    state_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))

    if args.repair and need_codes:
        py = sys.executable
        for i in range(0, len(need_codes), max(1, int(args.batch_size))):
            batch = need_codes[i : i + max(1, int(args.batch_size))]
            cmd = [
                py,
                "scripts/fetch_etf_cache.py",
                "--codes",
                *batch,
                "--source-order",
                "ths,eastmoney,akshare",
                "--crawler-only",
                "0",
                "--max-retries",
                "4",
                "--retry-sleep",
                "1",
                "--fresh-tolerance-days",
                "0",
                "--min-bootstrap-rows",
                "720",
                "--bootstrap-batch-size",
                "0",
                "--repair-rounds",
                "2",
                "--force-full-history",
                "1",
                "--jump-fail-threshold",
                "0.8",
                "--cooldown",
                "0.15",
            ]
            print(f"[repair] batch {i//max(1,int(args.batch_size))+1}: {len(batch)} codes")
            _run(cmd)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
