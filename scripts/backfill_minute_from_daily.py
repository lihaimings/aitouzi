from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import List

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
MINUTE_DIR = DATA / "minute"
STATE = ROOT / "state"

INTRADAY_15M_SLOTS = [
    "09:45:00",
    "10:00:00",
    "10:15:00",
    "10:30:00",
    "10:45:00",
    "11:00:00",
    "11:15:00",
    "11:30:00",
    "13:15:00",
    "13:30:00",
    "13:45:00",
    "14:00:00",
    "14:15:00",
    "14:30:00",
    "14:45:00",
    "15:00:00",
]


def _load_meta() -> pd.DataFrame:
    p = DATA / "etf_metadata.csv"
    if not p.exists():
        return pd.DataFrame(columns=["code", "listed_date"])
    d = pd.read_csv(p, dtype={"code": str})
    d["code"] = d["code"].astype(str).str.zfill(6)
    d["listed_date"] = pd.to_datetime(d.get("listed_date"), errors="coerce")
    return d[[c for c in ["code", "listed_date", "name"] if c in d.columns]].drop_duplicates("code", keep="last")


def _target_start(code: str, meta: pd.DataFrame, max_years: float) -> pd.Timestamp:
    now = pd.Timestamp(datetime.now())
    m = meta[meta["code"] == code]
    listed = pd.NaT if m.empty else m.iloc[0]["listed_date"]
    base = now - pd.Timedelta(days=int(365.25 * max_years))
    if pd.isna(listed):
        return base
    return max(base, pd.Timestamp(listed))


def _build_synth_15m_for_day(row: pd.Series) -> pd.DataFrame:
    day = pd.Timestamp(row["date"]).date()
    o, h, l, c = float(row["open"]), float(row["high"]), float(row["low"]), float(row["close"])
    v = float(row.get("volume", 0.0) or 0.0)
    a = float(row.get("amount", 0.0) or 0.0)
    n = len(INTRADAY_15M_SLOTS)

    bars = []
    for i, t in enumerate(INTRADAY_15M_SLOTS):
        dt = pd.Timestamp(f"{day} {t}")
        # Simple interpolation path from open to close
        p0 = o + (c - o) * (i / n)
        p1 = o + (c - o) * ((i + 1) / n)
        bo = p0
        bc = p1
        bh = max(bo, bc)
        bl = min(bo, bc)
        bars.append({
            "datetime": dt,
            "open": bo,
            "high": bh,
            "low": bl,
            "close": bc,
            "volume": v / n,
            "amount": a / n,
            "synthetic": 1,
        })

    # Inject daily high/low into two bars to preserve day envelope
    if bars:
        bars[3]["high"] = max(bars[3]["high"], h)
        bars[10]["low"] = min(bars[10]["low"], l)

    return pd.DataFrame(bars)


def main() -> int:
    ap = argparse.ArgumentParser(description="Backfill missing early 15m minute history using daily bars (synthetic)")
    ap.add_argument("--codes", nargs="+", required=True)
    ap.add_argument("--max-years", type=float, default=3.0)
    ap.add_argument("--period", default="15")
    args = ap.parse_args()

    meta = _load_meta()
    STATE.mkdir(parents=True, exist_ok=True)

    out_summary = {}

    for code in [str(c).zfill(6) for c in args.codes]:
        min_path = MINUTE_DIR / f"etf_{code}_{args.period}m.csv"
        day_path = DATA / f"etf_{code}.csv"
        if (not min_path.exists()) or (not day_path.exists()):
            out_summary[code] = {"ok": False, "reason": "missing minute or daily file"}
            continue

        mdf = pd.read_csv(min_path)
        if "datetime" not in mdf.columns:
            if "time" in mdf.columns:
                mdf = mdf.rename(columns={"time": "datetime"})
            else:
                out_summary[code] = {"ok": False, "reason": "minute file has no datetime"}
                continue

        mdf["datetime"] = pd.to_datetime(mdf["datetime"], errors="coerce")
        mdf = mdf.dropna(subset=["datetime"]).sort_values("datetime").reset_index(drop=True)
        if mdf.empty:
            out_summary[code] = {"ok": False, "reason": "minute empty"}
            continue

        earliest_min = pd.Timestamp(mdf["datetime"].min())
        target_start = _target_start(code, meta, args.max_years)

        if earliest_min <= target_start:
            out_summary[code] = {
                "ok": True,
                "message": "already covered",
                "earliest": str(earliest_min),
                "target_start": str(target_start),
                "added_rows": 0,
            }
            continue

        ddf = pd.read_csv(day_path)
        if "date" not in ddf.columns:
            out_summary[code] = {"ok": False, "reason": "daily file has no date"}
            continue

        for c in ["open", "high", "low", "close", "volume", "amount"]:
            if c in ddf.columns:
                ddf[c] = pd.to_numeric(ddf[c], errors="coerce")
            else:
                ddf[c] = 0.0
        ddf["date"] = pd.to_datetime(ddf["date"], errors="coerce")
        ddf = ddf.dropna(subset=["date", "open", "high", "low", "close"]).sort_values("date")

        gap_days = ddf[(ddf["date"] >= target_start.normalize()) & (ddf["date"] < earliest_min.normalize())].copy()
        if gap_days.empty:
            out_summary[code] = {"ok": True, "message": "no daily gap rows", "added_rows": 0}
            continue

        synth_parts: List[pd.DataFrame] = []
        for _, r in gap_days.iterrows():
            synth_parts.append(_build_synth_15m_for_day(r))
        sdf = pd.concat(synth_parts, ignore_index=True) if synth_parts else pd.DataFrame()

        if sdf.empty:
            out_summary[code] = {"ok": False, "reason": "failed build synthetic bars"}
            continue

        if "synthetic" not in mdf.columns:
            mdf["synthetic"] = 0
        out = pd.concat([mdf, sdf], ignore_index=True)
        out = out.sort_values("datetime").drop_duplicates(subset=["datetime"], keep="last").reset_index(drop=True)
        out.to_csv(min_path, index=False)

        out_summary[code] = {
            "ok": True,
            "earliest_before": str(earliest_min),
            "earliest_after": str(out["datetime"].min()),
            "target_start": str(target_start),
            "added_rows": int(len(sdf)),
            "synthetic": True,
            "path": str(min_path),
        }

    state_path = STATE / "minute_synthetic_backfill_summary.json"
    state_path.write_text(json.dumps({"updated_at": datetime.now().isoformat(timespec="seconds"), "codes": out_summary}, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(out_summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
