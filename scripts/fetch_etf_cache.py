import argparse
import hashlib
import json
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
from src.data_pipeline.eastmoney_loader import fetch_etf_daily as em_fetch
from src.data_pipeline.ths_loader import fetch_etf_daily as ths_fetch
from src.data_pipeline.tushare_loader import fetch_etf_daily as ts_fetch
from src.data_pipeline.universe import load_universe_codes

try:
    from src.data_pipeline.efinance_loader import fetch_etf_daily as ef_fetch
except Exception:
    ef_fetch = None

DATA_DIR = ROOT / "data"
BENCHMARK_CODES = ["510300", "510500", "159915"]
REPORTS_DIR = ROOT / "reports"
VERSIONS_PATH = DATA_DIR / "etf_data_versions.csv"
FETCH_HISTORY_PATH = REPORTS_DIR / "paper_rotation_fetch_history.csv"
WEIGHTS_PATH = REPORTS_DIR / "paper_rotation_weights.csv"


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


def _hash_dataframe(df: pd.DataFrame) -> str:
    if df is None or df.empty:
        return ""
    key_cols = [c for c in ["date", "open", "high", "low", "close", "volume", "amount"] if c in df.columns]
    if not key_cols:
        return ""
    sample = df[key_cols].tail(128).copy()
    if "date" in sample.columns:
        sample["date"] = pd.to_datetime(sample["date"], errors="coerce").astype(str)
    payload = sample.to_json(orient="records", force_ascii=False)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def _validate_fetched_data(
    fetched: pd.DataFrame,
    jump_fail_threshold: float,
    volume_spike_multiple: float,
) -> dict:
    fetched = fetched.copy()
    fetched["date"] = pd.to_datetime(fetched.get("date"), errors="coerce")
    fetched = fetched.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    if fetched.empty:
        raise RuntimeError("empty data after date normalization")

    close = pd.to_numeric(fetched.get("close"), errors="coerce")
    if close.isna().all():
        raise RuntimeError("close column all NaN")
    if bool((close <= 0).fillna(False).any()):
        raise RuntimeError("close<=0 detected")

    abs_ret = close.pct_change().abs()
    max_abs_jump = float(abs_ret.max()) if abs_ret.notna().any() else 0.0
    if max_abs_jump >= jump_fail_threshold:
        raise RuntimeError(f"extreme jump detected: {max_abs_jump:.4f}")

    volume = pd.to_numeric(fetched.get("volume"), errors="coerce")
    vol_ratio_max = 0.0
    if volume.notna().any():
        rolling_med = volume.rolling(20, min_periods=5).median()
        ratio = volume / rolling_med.replace(0, pd.NA)
        ratio = ratio.replace([float("inf"), -float("inf")], pd.NA)
        vol_ratio_max = float(ratio.max()) if ratio.notna().any() else 0.0

    anomaly_warn = vol_ratio_max >= volume_spike_multiple
    return {
        "max_abs_jump": round(max_abs_jump, 6),
        "volume_ratio_max": round(vol_ratio_max, 4),
        "anomaly_warn": bool(anomaly_warn),
    }


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
    def ths(code: str, start: str, end: str) -> pd.DataFrame:
        return ths_fetch(code, beg=start, end=end)

    def em(code: str, start: str, end: str) -> pd.DataFrame:
        return em_fetch(code, beg=start, end=end)

    def ak(code: str, start: str, end: str) -> pd.DataFrame:
        return ak_fetch(code, start_date=start, end_date=end)

    def bs(code: str, start: str, end: str) -> pd.DataFrame:
        start_iso = pd.to_datetime(start).strftime("%Y-%m-%d")
        end_iso = pd.to_datetime(end).strftime("%Y-%m-%d")
        return bs_fetch(code, start_date=start_iso, end_date=end_iso)

    def ts(code: str, _start: str, _end: str) -> pd.DataFrame:
        return ts_fetch(code)

    out: Dict[str, Callable[[str, str, str], pd.DataFrame]] = {
        "ths": ths,
        "eastmoney": em,
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
    skip_fallback_on_fresh: bool,
    jump_fail_threshold: float,
    volume_spike_multiple: float,
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
        if skip_fallback_on_fresh and is_fresh and source in {"efinance", "tushare", "baostock"}:
            errors.append(f"{source}:skipped_due_to_fresh_local_cache")
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

                quality_meta = _validate_fetched_data(
                    fetched=fetched,
                    jump_fail_threshold=jump_fail_threshold,
                    volume_spike_multiple=volume_spike_multiple,
                )

                short_history = existing.empty and len(fetched) < min_bootstrap_rows

                if not existing.empty:
                    old_last = pd.to_datetime(existing["date"].max())
                    new_last = pd.to_datetime(fetched["date"].max())
                    if pd.isna(new_last) or new_last < old_last - timedelta(days=1):
                        raise RuntimeError(f"fetched latest date too old: {new_last} < {old_last}")

                merged_n = _merged_rows(existing, fetched)
                short_after_merge = merged_n < min_bootstrap_rows

                out_path, old_rows, new_rows = _merge_and_save(code, existing, fetched)
                return {
                    "code": code,
                    "status": "short" if (short_history or short_after_merge) else "ok",
                    "source": source,
                    "rows_before": old_rows,
                    "rows_after": new_rows,
                    "latest_date": str(pd.to_datetime(fetched["date"].max()).date()),
                    "path": str(out_path),
                    "quality": quality_meta,
                    "content_hash": _hash_dataframe(fetched),
                    "short_history": bool(short_history or short_after_merge),
                }
            except Exception as e:
                err = f"{source}:retry{i+1}:{str(e)[:160]}"
                errors.append(err)
                if i < max_retries - 1:
                    time.sleep(retry_sleep * (2**i))

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
        "status": "待补全",
        "source": "none",
        "rows_before": 0,
        "rows_after": 0,
        "latest_date": None,
        "path": str(_canonical_path(code)),
        "errors": errors + ["all_sources_failed_need_retry"],
    }


def _save_versions(results: List[Dict], run_id: str) -> Path:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    rows = []
    ts = pd.Timestamp.now().isoformat()
    for r in results:
        if r.get("status") not in {"ok", "short", "stale"}:
            continue
        quality = r.get("quality", {}) if isinstance(r.get("quality"), dict) else {}
        rows.append(
            {
                "run_id": run_id,
                "updated_at": ts,
                "code": r.get("code"),
                "status": r.get("status"),
                "source": r.get("source"),
                "rows_after": r.get("rows_after"),
                "latest_date": r.get("latest_date"),
                "content_hash": r.get("content_hash", ""),
                "max_abs_jump": quality.get("max_abs_jump", None),
                "volume_ratio_max": quality.get("volume_ratio_max", None),
                "anomaly_warn": quality.get("anomaly_warn", False),
            }
        )

    new_df = pd.DataFrame(rows)
    if VERSIONS_PATH.exists():
        try:
            old_df = pd.read_csv(VERSIONS_PATH)
            out_df = pd.concat([old_df, new_df], ignore_index=True)
        except Exception:
            out_df = new_df
    else:
        out_df = new_df

    if not out_df.empty and "updated_at" in out_df.columns:
        out_df["updated_at"] = pd.to_datetime(out_df["updated_at"], errors="coerce")
        out_df = out_df.sort_values("updated_at").tail(200000)

    out_df.to_csv(VERSIONS_PATH, index=False)
    return VERSIONS_PATH


def _append_fetch_history(run_id: str, summary: pd.DataFrame, duration_sec: float) -> Path:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    counts = summary["status"].value_counts().to_dict() if not summary.empty else {}
    payload = {
        "run_id": run_id,
        "run_at": pd.Timestamp.now().isoformat(),
        "duration_sec": round(float(duration_sec), 3),
        "total": int(len(summary)),
        "ok": int(counts.get("ok", 0)),
        "short": int(counts.get("short", 0)),
        "stale": int(counts.get("stale", 0)),
        "failed": int(counts.get("failed", 0)),
        "pending_retry": int(counts.get("待补全", 0)),
        "queued": int(counts.get("queued", 0)),
        "success_ratio": round(float((counts.get("ok", 0) + counts.get("short", 0)) / max(1, len(summary))), 6),
        "error_total": int(summary["errors"].apply(lambda x: len(x) if isinstance(x, list) else 0).sum()) if "errors" in summary else 0,
    }
    row_df = pd.DataFrame([payload])
    if FETCH_HISTORY_PATH.exists():
        try:
            old = pd.read_csv(FETCH_HISTORY_PATH)
            out = pd.concat([old, row_df], ignore_index=True)
        except Exception:
            out = row_df
    else:
        out = row_df
    out.to_csv(FETCH_HISTORY_PATH, index=False)
    return FETCH_HISTORY_PATH


def _iter_codes(custom_codes: Optional[Iterable[str]], universe_size: int) -> List[str]:
    def normalize(code: str) -> str:
        s = str(code).strip()
        if s.endswith(".0"):
            s = s[:-2]
        return s.zfill(6) if s.isdigit() else s

    def held_codes() -> List[str]:
        if not WEIGHTS_PATH.exists():
            return []
        try:
            w = pd.read_csv(WEIGHTS_PATH)
            if "date" in w.columns:
                w = w.drop(columns=["date"])
            if w.empty:
                return []
            last = w.iloc[-1]
            pos = []
            for k, v in last.items():
                try:
                    if float(v) > 0:
                        pos.append(normalize(str(k)))
                except Exception:
                    continue
            return pos
        except Exception:
            return []

    base = list(custom_codes) if custom_codes else load_universe_codes(target_size=universe_size)
    if not base:
        base = list(ETF_LIST)

    ordered: List[str] = []
    seen = set()
    for code in held_codes() + [normalize(c) for c in base] + BENCHMARK_CODES:
        c = normalize(code)
        if len(c) == 6 and c.isdigit() and c not in seen:
            ordered.append(c)
            seen.add(c)
    return ordered


def _status_rank(status: str) -> int:
    ranking = {
        "待补全": 0,
        "queued": 0,
        "stale": 1,
        "short": 2,
        "ok": 3,
    }
    return int(ranking.get(str(status), -1))


def _rotate_order(order: List[str], shift: int) -> List[str]:
    if not order:
        return []
    k = shift % len(order)
    return order[k:] + order[:k]


def _run_fetch_pass(
    codes: List[str],
    source_order: List[str],
    max_retries: int,
    retry_sleep: float,
    backfill_days: int,
    min_bootstrap_rows: int,
    fresh_tolerance_days: int,
    skip_fallback_on_fresh: bool,
    jump_fail_threshold: float,
    volume_spike_multiple: float,
    cooldown: float,
) -> List[Dict]:
    out: List[Dict] = []
    for code in codes:
        r = _fetch_one_code(
            code=code,
            source_order=source_order,
            max_retries=max(1, max_retries),
            retry_sleep=max(0.0, retry_sleep),
            backfill_days=max(1, backfill_days),
            min_bootstrap_rows=max(1, min_bootstrap_rows),
            fresh_tolerance_days=max(0, fresh_tolerance_days),
            skip_fallback_on_fresh=bool(skip_fallback_on_fresh),
            jump_fail_threshold=max(0.01, float(jump_fail_threshold)),
            volume_spike_multiple=max(1.0, float(volume_spike_multiple)),
        )
        out.append(r)
        print(
            f"[{r['status']}] code={code} source={r.get('source')} rows={r.get('rows_before')}->{r.get('rows_after')} "
            f"latest={r.get('latest_date')}"
        )
        if r.get("status") != "ok" and r.get("errors"):
            print(f"  errors: {r['errors'][:3]}")

        sleep_s = max(0.0, cooldown)
        if r.get("status") in {"stale", "待补全"}:
            sleep_s = max(sleep_s, cooldown * 1.8)
        if sleep_s > 0:
            time.sleep(sleep_s)
    return out


def main() -> int:
    t0 = time.time()
    run_id = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    parser = argparse.ArgumentParser(description="Fetch ETF cache with robust free-source fallback")
    parser.add_argument("--codes", nargs="*", default=None, help="Optional ETF code list")
    parser.add_argument("--universe-size", type=int, default=200, help="Universe size when codes not provided")
    parser.add_argument("--limit", type=int, default=0, help="Optional cap on number of codes for quick checks")
    parser.add_argument("--source-order", default="ths,eastmoney,efinance,akshare,tushare,baostock")
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument("--retry-sleep", type=float, default=2.0)
    parser.add_argument("--backfill-days", type=int, default=10)
    parser.add_argument("--min-bootstrap-rows", type=int, default=240)
    parser.add_argument("--fresh-tolerance-days", type=int, default=3)
    parser.add_argument("--skip-fallback-on-fresh", type=int, default=0, help="1 means skip fallback sources when local cache is fresh")
    parser.add_argument("--cooldown", type=float, default=0.3)
    parser.add_argument("--bootstrap-batch-size", type=int, default=0, help="How many new symbols to bootstrap; 0 means full universe every run")
    parser.add_argument("--repair-rounds", type=int, default=2, help="Extra repair rounds for stale/pending symbols")
    parser.add_argument("--repair-source-order", default="baostock,akshare,ths,eastmoney,efinance,tushare")
    parser.add_argument("--repair-max-retries", type=int, default=4)
    parser.add_argument("--repair-cooldown-multiplier", type=float, default=1.5)
    parser.add_argument("--jump-fail-threshold", type=float, default=0.15, help="Fail fetch if absolute daily jump exceeds threshold")
    parser.add_argument("--volume-spike-multiple", type=float, default=10.0, help="Warn when volume spikes above rolling median multiple")
    parser.add_argument("--strict", action="store_true", help="Return non-zero when failed exists")
    args = parser.parse_args()

    codes = _iter_codes(args.codes, universe_size=max(20, args.universe_size))
    if args.limit and args.limit > 0:
        codes = codes[: args.limit]
    source_order = [x.strip() for x in args.source_order.split(",") if x.strip()]

    existing_codes = [c for c in codes if _canonical_path(c).exists()]
    new_codes = [c for c in codes if not _canonical_path(c).exists()]
    bootstrap_n = int(args.bootstrap_batch_size)
    if bootstrap_n <= 0:
        process_codes = list(codes)
        queued_new = []
    else:
        process_codes = existing_codes + new_codes[:bootstrap_n]
        queued_new = new_codes[bootstrap_n:]

    results_by_code: Dict[str, Dict] = {}
    first_pass = _run_fetch_pass(
        codes=process_codes,
        source_order=source_order,
        max_retries=max(1, args.max_retries),
        retry_sleep=max(0.0, args.retry_sleep),
        backfill_days=max(1, args.backfill_days),
        min_bootstrap_rows=max(1, args.min_bootstrap_rows),
        fresh_tolerance_days=max(0, args.fresh_tolerance_days),
        skip_fallback_on_fresh=bool(int(args.skip_fallback_on_fresh)),
        jump_fail_threshold=max(0.01, float(args.jump_fail_threshold)),
        volume_spike_multiple=max(1.0, float(args.volume_spike_multiple)),
        cooldown=max(0.0, args.cooldown),
    )
    for r in first_pass:
        results_by_code[str(r.get("code"))] = r

    repair_source_order = [x.strip() for x in str(args.repair_source_order).split(",") if x.strip()]
    repair_rounds = max(0, int(args.repair_rounds))
    repair_retry = max(int(args.max_retries), int(args.repair_max_retries))
    repair_cooldown = max(0.0, float(args.cooldown) * max(0.0, float(args.repair_cooldown_multiplier)))

    for round_i in range(repair_rounds):
        retry_codes = [
            c
            for c in process_codes
            if results_by_code.get(c, {}).get("status") in {"stale", "待补全"}
        ]
        if not retry_codes:
            break

        round_order = _rotate_order(repair_source_order or source_order, round_i)
        print(
            f"\n[repair round {round_i + 1}/{repair_rounds}] retry_codes={len(retry_codes)} "
            f"source_order={','.join(round_order)}"
        )
        repaired = _run_fetch_pass(
            codes=retry_codes,
            source_order=round_order,
            max_retries=repair_retry,
            retry_sleep=max(0.0, args.retry_sleep),
            backfill_days=max(1, args.backfill_days),
            min_bootstrap_rows=max(1, args.min_bootstrap_rows),
            fresh_tolerance_days=max(0, args.fresh_tolerance_days),
            skip_fallback_on_fresh=bool(int(args.skip_fallback_on_fresh)),
            jump_fail_threshold=max(0.01, float(args.jump_fail_threshold)),
            volume_spike_multiple=max(1.0, float(args.volume_spike_multiple)),
            cooldown=repair_cooldown,
        )
        for r in repaired:
            code = str(r.get("code"))
            old = results_by_code.get(code)
            if old is None:
                results_by_code[code] = r
                continue
            if _status_rank(r.get("status")) >= _status_rank(old.get("status")):
                results_by_code[code] = r

    results = [results_by_code[c] for c in process_codes if c in results_by_code]

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
    short_n = int((summary["status"] == "short").sum()) if not summary.empty else 0
    stale_n = int((summary["status"] == "stale").sum()) if not summary.empty else 0
    fail_n = int((summary["status"] == "failed").sum()) if not summary.empty else 0
    pending_n = int((summary["status"] == "待补全").sum()) if not summary.empty else 0
    queued_n = int((summary["status"] == "queued").sum()) if not summary.empty else 0

    out_json = ROOT / "reports" / "paper_rotation_fetch_status.json"
    out_csv = ROOT / "reports" / "paper_rotation_fetch_status.csv"
    out_json.parent.mkdir(parents=True, exist_ok=True)
    summary.to_json(out_json, orient="records", force_ascii=False, indent=2)
    summary.to_csv(out_csv, index=False, encoding="utf-8-sig")
    versions_path = _save_versions(results=results, run_id=run_id)
    history_path = _append_fetch_history(run_id=run_id, summary=summary, duration_sec=time.time() - t0)

    print("\n抓数结果汇总:")
    print(f"- 成功更新: {ok_n}")
    print(f"- 短历史(可用待积累): {short_n}")
    print(f"- 沿用旧缓存: {stale_n}")
    print(f"- 失败: {fail_n}")
    print(f"- 待补全: {pending_n}")
    print(f"- 排队待补抓: {queued_n}")
    print(f"- 数据版本记录: {versions_path}")
    print(f"- 抓数历史记录: {history_path}")
    print(f"- 状态JSON: {out_json}")
    print(f"- 状态CSV: {out_csv}")

    if args.strict and (fail_n > 0 or pending_n > 0):
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
