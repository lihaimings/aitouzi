from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
REPORTS = ROOT / "reports"
STATE = ROOT / "state"


def load_codes(layer: str) -> list[str]:
    p = DATA / "layers" / ("l1_watch_codes.csv" if layer == "l1" else "l2_core_codes.csv")
    if not p.exists():
        return []
    d = pd.read_csv(p, dtype={"code": str})
    return sorted({str(x).zfill(6) for x in d.get("code", []).dropna().tolist()})


def minute_years(code: str) -> tuple[float, str | None, str | None]:
    p = DATA / "minute" / f"etf_{code}_15m.csv"
    if not p.exists():
        return 0.0, None, None
    try:
        d = pd.read_csv(p)
    except Exception:
        return 0.0, None, None
    col = "datetime" if "datetime" in d.columns else ("time" if "time" in d.columns else None)
    if not col:
        return 0.0, None, None
    dt = pd.to_datetime(d[col], errors="coerce").dropna()
    if dt.empty:
        return 0.0, None, None
    y = (dt.max() - dt.min()).days / 365.25
    return float(y), str(dt.min()), str(dt.max())


def daily_years(code: str) -> tuple[float, str | None, str | None]:
    p = DATA / f"etf_{code}.csv"
    if not p.exists():
        return 0.0, None, None
    try:
        d = pd.read_csv(p, usecols=["date"])
    except Exception:
        return 0.0, None, None
    dt = pd.to_datetime(d["date"], errors="coerce").dropna()
    if dt.empty:
        return 0.0, None, None
    y = (dt.max() - dt.min()).days / 365.25
    return float(y), str(dt.min()), str(dt.max())


def build_layer(layer: str, minute_threshold_years: float = 1.0):
    rows = []
    for c in load_codes(layer):
        my, ms, me = minute_years(c)
        dy, ds, de = daily_years(c)
        source = "minute_15m" if my >= minute_threshold_years else "daily"
        rows.append({
            "layer": layer,
            "code": c,
            "selected_source": source,
            "minute_years": round(my, 3),
            "minute_start": ms,
            "minute_end": me,
            "daily_years": round(dy, 3),
            "daily_start": ds,
            "daily_end": de,
        })
    return pd.DataFrame(rows)


def main():
    REPORTS.mkdir(parents=True, exist_ok=True)
    STATE.mkdir(parents=True, exist_ok=True)

    l1 = build_layer("l1", 1.0)
    l2 = build_layer("l2", 1.0)
    all_df = pd.concat([l1, l2], ignore_index=True)

    out_csv = REPORTS / "backtest_data_plan.csv"
    all_df.to_csv(out_csv, index=False, encoding="utf-8-sig")

    summary = {
        "updated_at": datetime.now().isoformat(timespec="seconds"),
        "rule": "minute>=1y use minute_15m else use daily",
        "l1": {
            "total": int(len(l1)),
            "minute_selected": int((l1["selected_source"] == "minute_15m").sum()),
            "daily_selected": int((l1["selected_source"] == "daily").sum()),
        },
        "l2": {
            "total": int(len(l2)),
            "minute_selected": int((l2["selected_source"] == "minute_15m").sum()),
            "daily_selected": int((l2["selected_source"] == "daily").sum()),
        },
        "report_csv": str(out_csv),
    }

    out_json = STATE / "backtest_data_plan_summary.json"
    out_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
