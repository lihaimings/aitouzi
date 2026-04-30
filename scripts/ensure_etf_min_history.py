from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path
from typing import Dict, List

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
REPORTS_DIR = ROOT / "reports"


def _load_universe_codes() -> List[str]:
    p = DATA_DIR / "etf_universe.csv"
    if not p.exists():
        return []
    df = pd.read_csv(p, dtype={"code": str})
    codes = [str(x).zfill(6) for x in df.get("code", []).tolist()]
    return sorted(set([c for c in codes if len(c) == 6 and c.isdigit()]))


def _best_cache_stats(codes: List[str]) -> pd.DataFrame:
    code_set = set(codes)
    best: Dict[str, Dict] = {}
    for p in DATA_DIR.glob("etf_*.csv"):
        m = re.match(r"etf_(\d{6})(?:_.*)?\.csv$", p.name)
        if not m:
            continue
        code = m.group(1)
        if code not in code_set:
            continue
        try:
            d = pd.read_csv(p, usecols=["date"])
        except Exception:
            continue
        dt = pd.to_datetime(d["date"], errors="coerce").dropna()
        if dt.empty:
            continue
        rows = int(len(dt))
        start = pd.Timestamp(dt.min())
        end = pd.Timestamp(dt.max())
        years = float((end - start).days / 365.25)
        rec = best.get(code)
        if rec is None or rows > int(rec.get("cache_rows", 0)):
            best[code] = {
                "code": code,
                "cache_rows": rows,
                "cache_start": str(start.date()),
                "cache_end": str(end.date()),
                "cache_years": round(years, 3),
                "cache_file": str(p),
            }

    out = pd.DataFrame([best[c] for c in sorted(best.keys())])
    if out.empty:
        return pd.DataFrame(columns=["code", "cache_rows", "cache_start", "cache_end", "cache_years", "cache_file"])
    return out


def _run(cmd: List[str]) -> None:
    print("[run]", " ".join(cmd))
    result = subprocess.run(cmd, cwd=str(ROOT), capture_output=True, text=True)
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr)
    if result.returncode != 0:
        raise RuntimeError(f"command failed: {' '.join(cmd)}")


def _batched(items: List[str], n: int) -> List[List[str]]:
    out = []
    for i in range(0, len(items), n):
        out.append(items[i : i + n])
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Ensure ETF cache history is at least target years, and collect listed dates")
    parser.add_argument("--min-years", type=float, default=2.0)
    parser.add_argument("--trading-days-per-year", type=int, default=240)
    parser.add_argument("--batch-size", type=int, default=25)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    codes = _load_universe_codes()
    if not codes:
        print("no universe codes found")
        return 1

    min_rows = int(max(1, args.min_years * args.trading_days_per_year))

    before = _best_cache_stats(codes)
    merged_before = pd.DataFrame({"code": codes}).merge(before, on="code", how="left")
    merged_before["cache_rows"] = pd.to_numeric(merged_before["cache_rows"], errors="coerce").fillna(0).astype(int)
    merged_before["cache_years"] = pd.to_numeric(merged_before["cache_years"], errors="coerce").fillna(0.0)
    need_backfill = merged_before[
        (merged_before["cache_rows"] < min_rows) | (merged_before["cache_years"] < float(args.min_years))
    ]["code"].astype(str).tolist()

    print(f"universe={len(codes)}, need_backfill(<{args.min_years}y)={len(need_backfill)}")

    if not args.dry_run and need_backfill:
        py = sys.executable
        for batch in _batched(need_backfill, max(1, int(args.batch_size))):
            cmd = [
                py,
                "scripts/fetch_etf_cache.py",
                "--codes",
                *batch,
                "--source-order",
                "ths,eastmoney",
                "--crawler-only",
                "1",
                "--max-retries",
                "4",
                "--retry-sleep",
                "1",
                "--fresh-tolerance-days",
                "0",
                "--skip-fallback-on-fresh",
                "0",
                "--min-bootstrap-rows",
                str(min_rows),
                "--bootstrap-batch-size",
                "0",
                "--repair-rounds",
                "2",
                "--repair-max-retries",
                "5",
                "--repair-cooldown-multiplier",
                "1.3",
                "--force-full-history",
                "1",
                "--jump-fail-threshold",
                "0.8",
                "--cooldown",
                "0.15",
            ]
            _run(cmd)

    # build metadata (listed_date + cache depth)
    _run([sys.executable, "scripts/build_etf_metadata.py", "--scope", "universe"])

    after = _best_cache_stats(codes)
    merged = pd.DataFrame({"code": codes}).merge(after, on="code", how="left")
    merged["cache_rows"] = pd.to_numeric(merged["cache_rows"], errors="coerce").fillna(0).astype(int)
    merged["cache_years"] = pd.to_numeric(merged["cache_years"], errors="coerce").fillna(0.0)

    meta_path = DATA_DIR / "etf_metadata.csv"
    if meta_path.exists():
        meta = pd.read_csv(meta_path, dtype={"code": str})
        meta_cols = [c for c in ["code", "name", "listed_date", "listed_years", "meta_source", "likely_history_shortfall"] if c in meta.columns]
        merged = merged.merge(meta[meta_cols], on="code", how="left")
    else:
        merged["name"] = ""
        merged["listed_date"] = ""
        merged["listed_years"] = ""
        merged["meta_source"] = ""
        merged["likely_history_shortfall"] = False

    merged["listed_date"] = pd.to_datetime(merged.get("listed_date"), errors="coerce")
    merged["cache_start"] = pd.to_datetime(merged.get("cache_start"), errors="coerce")
    merged["gap_days"] = (merged["cache_start"] - merged["listed_date"]).dt.days
    merged["gap_months"] = (merged["gap_days"] / 30.4375).round(1)

    merged["need_more_history"] = (merged["cache_rows"] < min_rows) | (merged["cache_years"] < float(args.min_years))
    report = merged[merged["need_more_history"] == True].copy()
    report = report.sort_values(["cache_years", "cache_rows"], ascending=[True, True])

    out = REPORTS_DIR / "etf_min_history_gap.csv"
    report.to_csv(out, index=False, encoding="utf-8-sig")

    print(f"after backfill still <{args.min_years}y: {len(report)}")
    print(f"report: {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
