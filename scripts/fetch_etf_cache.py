import argparse
import sys
import time
from datetime import timedelta
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Tuple

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.data_pipeline.akshare_loader import ETF_LIST
from src.data_pipeline.akshare_loader import fetch_etf_daily as ak_fetch
from src.data_pipeline.baostock_loader import fetch_k_daily as bs_fetch
from src.data_pipeline.tushare_loader import fetch_etf_daily as ts_fetch
from src.data_pipeline.universe import load_universe_codes

try:
    from src.data_pipeline.efinance_loader import fetch_etf_daily as ef_fetch
except Exception:
    ef_fetch = None

DATA_DIR = ROOT / "data"
BENCHMARK_CODES = ["510300", "510500", "159915"]


def _today_ymd() -> str:
    return pd.Timestamp.today().strftime("%Y%m%d")


def _canonical_path(code: str) -> Path:
    return DATA_DIR / f"etf_{code}.csv"


def _read_existing(code: str) -> pd.DataFrame:
    path = _canonical_path(code)
    if not path.exists():
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume", "amount"])
    try:
        df = pd.read_csv(path)
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        for col in ["open", "high", "low", "close", "volume", "amount"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        return df.dropna(subset=["date", "close"]).sort_values("date").reset_index(drop=True)
    except Exception:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume", "amount"])


def _merge_and_save(code: str, old_df: pd.DataFrame, new_df: pd.DataFrame) -> Tuple[Path, int, int]:
    merged = pd.concat([old_df, new_df], axis=0, ignore_index=True)
    merged = merged.dropna(subset=["date", "close"]).sort_values("date").drop_duplicates(subset=["date"], keep="last")

    for col in ["open", "high", "low", "close", "volume", "amount"]:
        if col in merged.columns:
            merged[col] = pd.to_numeric(merged[col], errors="coerce")

    out = _canonical_path(code)
    merged.to_csv(out, index=False)
    try:
        merged.to_parquet(DATA_DIR / f"etf_{code}.parquet", index=False)
    except Exception:
        pass

    return out, int(len(old_df)), int(len(merged))


def _merged_rows(old_df: pd.DataFrame, new_df: pd.DataFrame) -> int:
    merged = pd.concat([old_df, new_df], axis=0, ignore_index=True)
    merged = merged.dropna(subset=["date", "close"]).sort_values("date").drop_duplicates(subset=["date"], keep="last")
    return int(len(merged))


def _compute_start_date(existing: pd.DataFrame, backfill_days: int) -> str:
    if existing.empty:
        return "20100101"
    last_date = pd.to_datetime(existing["date"].max())
    start = (last_date - timedelta(days=backfill_days)).strftime("%Y%m%d")
    return start


def _source_funcs() -> Dict[str, Callable[[str, str, str], pd.DataFrame]]:
    def ak(code: str, start: str, end: str) -> pd.DataFrame:
        return ak_fetch(code, start_date=start, end_date=end)

    def bs(code: str, start: str, end: str) -> pd.DataFrame:
        start_iso = pd.to_datetime(start).strftime("%Y-%m-%d")
        end_iso = pd.to_datetime(end).strftime("%Y-%m-%d")
        return bs_fetch(code, start_date=start_iso, end_date=end_iso)

    def ts(code: str, _start: str, _end: str) -> pd.DataFrame:
        return ts_fetch(code)

    out: Dict[str, Callable[[str, str, str], pd.DataFrame]] = {
        "akshare": ak,
        "baostock": bs,
        "tushare": ts,
    }
    if ef_fetch is not None:
        out["efinance"] = lambda code, start, end: ef_fetch(code, beg=start, end=end)
    return out


def _fetch_one_code(
    code: str,
    source_order: List[str],
    max_retries: int,
    retry_sleep: float,
    backfill_days: int,
    min_bootstrap_rows: int,
    fresh_tolerance_days: int,
) -> Dict:
    existing = _read_existing(code)
    start = _compute_start_date(existing, backfill_days=backfill_days)
    end = _today_ymd()
    funcs = _source_funcs()

    existing_latest = None
    is_fresh = False
    if not existing.empty:
        existing_latest = pd.to_datetime(existing["date"].max())
        fresh_cutoff = pd.Timestamp.today().normalize() - timedelta(days=max(0, fresh_tolerance_days))
        is_fresh = bool(existing_latest >= fresh_cutoff and len(existing) >= min_bootstrap_rows)

    errors: List[str] = []
    for source in source_order:
        if is_fresh and source in {"efinance", "tushare", "baostock"}:
            errors.append(f"{source}:skipped_due_to_fresh_local_cache")
            continue

        if source == "baostock" and not existing.empty and len(existing) < min_bootstrap_rows:
            errors.append("baostock:skipped_for_low_history_cache")
            continue

        func = funcs.get(source)
        if func is None:
            errors.append(f"{source}:not_available")
            continue

        for i in range(max_retries):
            try:
                fetched = func(code, start, end)
                if fetched is None or fetched.empty:
                    raise RuntimeError("empty data")

                if existing.empty and len(fetched) < min_bootstrap_rows:
                    raise RuntimeError(f"insufficient bootstrap rows: {len(fetched)} < {min_bootstrap_rows}")

                if not existing.empty:
                    old_last = pd.to_datetime(existing["date"].max())
                    new_last = pd.to_datetime(fetched["date"].max())
                    if pd.isna(new_last) or new_last < old_last - timedelta(days=1):
                        raise RuntimeError(f"fetched latest date too old: {new_last} < {old_last}")

                merged_n = _merged_rows(existing, fetched)
                if merged_n < min_bootstrap_rows:
                    raise RuntimeError(f"total rows too small after merge: {merged_n} < {min_bootstrap_rows}")

                out_path, old_rows, new_rows = _merge_and_save(code, existing, fetched)
                return {
                    "code": code,
                    "status": "ok",
                    "source": source,
                    "rows_before": old_rows,
                    "rows_after": new_rows,
                    "latest_date": str(pd.to_datetime(fetched["date"].max()).date()),
                    "path": str(out_path),
                }
            except Exception as e:
                err = f"{source}:retry{i+1}:{str(e)[:160]}"
                errors.append(err)
                if i < max_retries - 1:
                    time.sleep(retry_sleep * (i + 1))

    if not existing.empty:
        return {
            "code": code,
            "status": "stale",
            "source": "none",
            "rows_before": int(len(existing)),
            "rows_after": int(len(existing)),
            "latest_date": str(pd.to_datetime(existing["date"].max()).date()),
            "path": str(_canonical_path(code)),
            "errors": errors,
        }

    return {
        "code": code,
        "status": "failed",
        "source": "none",
        "rows_before": 0,
        "rows_after": 0,
        "latest_date": None,
        "path": str(_canonical_path(code)),
        "errors": errors,
    }


def _iter_codes(custom_codes: Optional[Iterable[str]], universe_size: int) -> List[str]:
    base = list(custom_codes) if custom_codes else load_universe_codes(target_size=universe_size)
    if not base:
        base = list(ETF_LIST)
    return sorted(set(base + BENCHMARK_CODES))


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch ETF cache with robust free-source fallback")
    parser.add_argument("--codes", nargs="*", default=None, help="Optional ETF code list")
    parser.add_argument("--universe-size", type=int, default=200, help="Universe size when codes not provided")
    parser.add_argument("--limit", type=int, default=0, help="Optional cap on number of codes for quick checks")
    parser.add_argument("--source-order", default="akshare,efinance,tushare,baostock")
    parser.add_argument("--max-retries", type=int, default=2)
    parser.add_argument("--retry-sleep", type=float, default=2.0)
    parser.add_argument("--backfill-days", type=int, default=10)
    parser.add_argument("--min-bootstrap-rows", type=int, default=240)
    parser.add_argument("--fresh-tolerance-days", type=int, default=3)
    parser.add_argument("--cooldown", type=float, default=0.3)
    parser.add_argument("--bootstrap-batch-size", type=int, default=20, help="How many new symbols (without local cache) to bootstrap per run")
    parser.add_argument("--strict", action="store_true", help="Return non-zero when failed exists")
    args = parser.parse_args()

    codes = _iter_codes(args.codes, universe_size=max(20, args.universe_size))
    if args.limit and args.limit > 0:
        codes = codes[: args.limit]
    source_order = [x.strip() for x in args.source_order.split(",") if x.strip()]

    existing_codes = [c for c in codes if _canonical_path(c).exists()]
    new_codes = [c for c in codes if not _canonical_path(c).exists()]
    bootstrap_n = max(0, int(args.bootstrap_batch_size))
    process_codes = existing_codes + new_codes[:bootstrap_n]
    queued_new = new_codes[bootstrap_n:]

    results = []
    for code in process_codes:
        r = _fetch_one_code(
            code=code,
            source_order=source_order,
            max_retries=max(1, args.max_retries),
            retry_sleep=max(0.0, args.retry_sleep),
            backfill_days=max(1, args.backfill_days),
            min_bootstrap_rows=max(1, args.min_bootstrap_rows),
            fresh_tolerance_days=max(0, args.fresh_tolerance_days),
        )
        results.append(r)
        print(
            f"[{r['status']}] code={code} source={r.get('source')} rows={r.get('rows_before')}->{r.get('rows_after')} "
            f"latest={r.get('latest_date')}"
        )
        if r.get("status") != "ok" and r.get("errors"):
            print(f"  errors: {r['errors'][:3]}")
        time.sleep(max(0.0, args.cooldown))

    for code in queued_new:
        results.append(
            {
                "code": code,
                "status": "queued",
                "source": "none",
                "rows_before": 0,
                "rows_after": 0,
                "latest_date": None,
                "path": str(_canonical_path(code)),
                "errors": ["queued_for_next_bootstrap_batch"],
            }
        )

    summary = pd.DataFrame(results)
    ok_n = int((summary["status"] == "ok").sum()) if not summary.empty else 0
    stale_n = int((summary["status"] == "stale").sum()) if not summary.empty else 0
    fail_n = int((summary["status"] == "failed").sum()) if not summary.empty else 0
    queued_n = int((summary["status"] == "queued").sum()) if not summary.empty else 0

    out_json = ROOT / "reports" / "paper_rotation_fetch_status.json"
    out_csv = ROOT / "reports" / "paper_rotation_fetch_status.csv"
    out_json.parent.mkdir(parents=True, exist_ok=True)
    summary.to_json(out_json, orient="records", force_ascii=False, indent=2)
    summary.to_csv(out_csv, index=False)

    print("\nfetch summary:")
    print(f"- ok: {ok_n}")
    print(f"- stale: {stale_n}")
    print(f"- failed: {fail_n}")
    print(f"- queued: {queued_n}")
    print(f"- status_json: {out_json}")
    print(f"- status_csv: {out_csv}")

    if args.strict and fail_n > 0:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
