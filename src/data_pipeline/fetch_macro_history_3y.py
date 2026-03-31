import argparse
import json
from datetime import datetime, timedelta, timezone
from io import StringIO
from pathlib import Path

import pandas as pd
import requests

BASE = Path(__file__).resolve().parents[2]
CFG_PATH = Path(__file__).resolve().parent / "macro_history_series.json"


def fetch_one(series_id: str, start_date: str, end_date: str) -> pd.DataFrame:
    url = "https://fred.stlouisfed.org/graph/fredgraph.csv"
    resp = requests.get(url, params={"id": series_id, "cosd": start_date, "coed": end_date}, timeout=25)
    resp.raise_for_status()
    df = pd.read_csv(StringIO(resp.text))
    date_col = "DATE" if "DATE" in df.columns else ("observation_date" if "observation_date" in df.columns else df.columns[0])
    value_col = [c for c in df.columns if c != date_col][0]
    df = df.rename(columns={date_col: "date", value_col: "value"})
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    return df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--years", type=int, default=None)
    args = parser.parse_args()

    cfg = json.loads(CFG_PATH.read_text(encoding="utf-8"))
    years = args.years or int(cfg.get("default_years", 3))
    end_date = datetime.now(timezone.utc).date()
    start_date = end_date - timedelta(days=365 * years)

    raw_dir = BASE / "data" / "layers" / "raw" / "macro_history"
    proc_dir = BASE / "data" / "layers" / "processed" / "macro_history"
    state_dir = BASE / "state"
    raw_dir.mkdir(parents=True, exist_ok=True)
    proc_dir.mkdir(parents=True, exist_ok=True)
    state_dir.mkdir(parents=True, exist_ok=True)

    status = {"updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "years": years, "series": {}}
    merged = None

    for s in cfg.get("series", []):
        sid, name = s["id"], s["name"]
        try:
            df = fetch_one(sid, start_date.isoformat(), end_date.isoformat())
            df.to_csv(raw_dir / f"{sid}.csv", index=False, encoding="utf-8")
            series_df = df[["date", "value"]].rename(columns={"value": name})
            merged = series_df if merged is None else merged.merge(series_df, on="date", how="outer")
            status["series"][sid] = {
                "ok": True,
                "rows": int(len(df)),
                "min_date": str(df["date"].min().date()),
                "max_date": str(df["date"].max().date()),
            }
        except Exception as e:
            status["series"][sid] = {"ok": False, "error": str(e)}

    ok_count = sum(1 for v in status["series"].values() if v.get("ok"))
    if merged is not None:
        merged = merged.sort_values("date").reset_index(drop=True)
        out = proc_dir / "macro_history_3y_merged.csv"
        merged.to_csv(out, index=False, encoding="utf-8")
        summary = {
            "ok": True,
            "series_total": len(status["series"]),
            "series_ok": ok_count,
            "success_rate": round(ok_count / max(len(status["series"]), 1), 4),
            "merged_rows": int(len(merged)),
            "merged_min_date": str(merged["date"].min().date()),
            "merged_max_date": str(merged["date"].max().date()),
            "merged_path": str(out),
        }
    else:
        summary = {
            "ok": False,
            "series_total": len(status["series"]),
            "series_ok": ok_count,
            "success_rate": round(ok_count / max(len(status["series"]), 1), 4),
            "merged_rows": 0,
        }

    status["summary"] = summary
    state_path = state_dir / "macro_history_3y_status.json"
    state_path.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    if not summary.get("ok"):
        raise SystemExit(2)


if __name__ == "__main__":
    main()
