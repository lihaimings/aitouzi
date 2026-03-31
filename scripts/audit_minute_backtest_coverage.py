from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import List

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
STATE = ROOT / "state"
REPORTS = ROOT / "reports"


def load_codes(layer: str) -> List[str]:
    p = DATA / "layers" / ("l1_watch_codes.csv" if layer == "l1" else "l2_core_codes.csv")
    if not p.exists():
        return []
    d = pd.read_csv(p, dtype={"code": str})
    return sorted({str(x).zfill(6) for x in d.get("code", []).dropna().tolist()})


def load_meta() -> pd.DataFrame:
    p = DATA / "etf_metadata.csv"
    if not p.exists():
        return pd.DataFrame(columns=["code", "listed_date"])
    d = pd.read_csv(p, dtype={"code": str})
    d["code"] = d["code"].map(lambda x: str(x).zfill(6))
    d["listed_date"] = pd.to_datetime(d.get("listed_date"), errors="coerce")
    return d[[c for c in ["code", "listed_date", "name"] if c in d.columns]].drop_duplicates("code", keep="last")


def cache_range_minute(code: str, period: str) -> tuple[pd.Timestamp | None, pd.Timestamp | None, int]:
    p = DATA / "minute" / f"etf_{code}_{period}m.csv"
    if not p.exists():
        return None, None, 0
    try:
        d = pd.read_csv(p)
    except Exception:
        return None, None, 0
    cands = [c for c in ["datetime", "time", "日期"] if c in d.columns]
    if not cands:
        return None, None, len(d)
    dt = pd.to_datetime(d[cands[0]], errors="coerce").dropna()
    if dt.empty:
        return None, None, len(d)
    return pd.Timestamp(dt.min()), pd.Timestamp(dt.max()), int(len(dt))


def audit_layer(layer: str, period: str, max_years: float, recent_days: int, tolerance_days: int) -> pd.DataFrame:
    codes = load_codes(layer)
    meta = load_meta()
    now = pd.Timestamp(datetime.now().date())
    cutoff_recent = now - pd.Timedelta(days=recent_days)
    cutoff_3y = now - pd.Timedelta(days=int(365.25 * max_years))

    rows = []
    for code in codes:
        m = meta[meta["code"] == code]
        listed = pd.NaT if m.empty else m.iloc[0]["listed_date"]
        if pd.isna(listed):
            listed = cutoff_3y
        target_start = max(listed, cutoff_3y)

        s, e, n = cache_range_minute(code, period)
        start_ok = (s is not None) and (s <= target_start + pd.Timedelta(days=tolerance_days))
        end_ok = (e is not None) and (e >= cutoff_recent)
        ok = bool(start_ok and end_ok and n > 0)

        rows.append({
            "layer": layer,
            "code": code,
            "listed_date": str(listed.date()) if not pd.isna(listed) else None,
            "target_start": str(target_start.date()),
            "minute_start": str(s) if s is not None else None,
            "minute_end": str(e) if e is not None else None,
            "rows": n,
            "start_ok": bool(start_ok),
            "end_ok": bool(end_ok),
            "ok": ok,
        })
    return pd.DataFrame(rows)


def main() -> int:
    ap = argparse.ArgumentParser(description="Audit minute backtest coverage for L1/L2")
    ap.add_argument("--period", default="15")
    ap.add_argument("--max-years", type=float, default=3.0)
    ap.add_argument("--recent-days", type=int, default=3)
    ap.add_argument("--start-tolerance-days", type=int, default=10)
    args = ap.parse_args()

    REPORTS.mkdir(parents=True, exist_ok=True)
    STATE.mkdir(parents=True, exist_ok=True)

    l1 = audit_layer("l1", args.period, args.max_years, args.recent_days, args.start_tolerance_days)
    l2 = audit_layer("l2", args.period, args.max_years, args.recent_days, args.start_tolerance_days)
    all_df = pd.concat([l1, l2], ignore_index=True)

    out_csv = REPORTS / f"minute_backtest_coverage_{args.period}m.csv"
    all_df.to_csv(out_csv, index=False, encoding="utf-8-sig")

    def s(df):
        return {
            "total": int(len(df)),
            "ok": int(df["ok"].sum()) if len(df) else 0,
            "need_repair": int((~df["ok"]).sum()) if len(df) else 0,
            "need_repair_codes": df.loc[~df["ok"], "code"].astype(str).tolist() if len(df) else [],
        }

    summary = {
        "updated_at": datetime.now().isoformat(timespec="seconds"),
        "period": args.period,
        "rule": "target_start=max(listed_date, now-3y), end>=now-recent_days",
        "l1": s(l1),
        "l2": s(l2),
        "report_csv": str(out_csv),
    }

    state_path = STATE / f"minute_backtest_coverage_{args.period}m.json"
    state_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if summary["l1"]["need_repair"] == 0 and summary["l2"]["need_repair"] == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
