import argparse
import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.reporting.feishu_push import push_dm

REPORTS = ROOT / "reports"
DATA = ROOT / "data"


def _load_config() -> dict:
    cfg_path = ROOT / "config.yaml"
    if not cfg_path.exists():
        return {}
    try:
        import yaml  # type: ignore

        d = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}


def _safe_read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def _evaluate_fetch_status(df: pd.DataFrame) -> dict:
    if df.empty or "status" not in df.columns:
        return {"status": "FAIL", "reason": "missing_fetch_status", "stats": {}}

    total = max(1, len(df))
    counts = df["status"].value_counts().to_dict()
    fail_ratio = float(counts.get("failed", 0) / total)
    stale_ratio = float(counts.get("stale", 0) / total)
    queued_ratio = float(counts.get("queued", 0) / total)

    status = "PASS"
    reason = ""
    if fail_ratio > 0.20:
        status = "FAIL"
        reason = "fetch_failed_ratio_too_high"
    elif stale_ratio > 0.70 or queued_ratio > 0.80:
        status = "WARN"
        reason = "fetch_stale_or_queued_high"

    return {
        "status": status,
        "reason": reason,
        "stats": {
            "total": int(total),
            "counts": counts,
            "fail_ratio": round(fail_ratio, 4),
            "stale_ratio": round(stale_ratio, 4),
            "queued_ratio": round(queued_ratio, 4),
        },
    }


def _evaluate_cache_coverage(
    fresh_days: int = 5,
    min_rows: int = 240,
    min_usable_ratio_fail: float = 0.03,
    min_usable_ratio_warn: float = 0.10,
    min_usable_count_fail: int = 5,
) -> dict:
    universe_path = DATA / "etf_universe.csv"
    if not universe_path.exists():
        return {"status": "FAIL", "reason": "missing_universe_file", "stats": {}}

    try:
        universe = pd.read_csv(universe_path, dtype={"code": str})
        universe_codes = [str(x).strip().zfill(6) for x in universe["code"].tolist()]
    except Exception:
        return {"status": "FAIL", "reason": "invalid_universe_file", "stats": {}}

    usable = 0
    fresh = 0
    for code in universe_codes:
        f = DATA / f"etf_{code}.csv"
        if not f.exists():
            continue
        try:
            df = pd.read_csv(f, usecols=["date"])
            rows = len(df)
            latest = pd.to_datetime(df["date"], errors="coerce").max()
            if rows >= min_rows:
                usable += 1
            if pd.notna(latest) and latest >= (pd.Timestamp.today().normalize() - pd.Timedelta(days=fresh_days)):
                fresh += 1
        except Exception:
            continue

    total = max(1, len(universe_codes))
    usable_ratio = usable / total
    fresh_ratio = fresh / total
    status = "PASS"
    reason = ""
    if usable < int(min_usable_count_fail) or usable_ratio < float(min_usable_ratio_fail):
        status = "FAIL"
        reason = "usable_cache_too_low"
    elif usable_ratio < float(min_usable_ratio_warn) or fresh_ratio < 0.20:
        status = "WARN"
        reason = "cache_coverage_low"

    return {
        "status": status,
        "reason": reason,
        "stats": {
            "universe_total": int(total),
            "usable_count": int(usable),
            "fresh_count": int(fresh),
            "usable_ratio": round(usable_ratio, 4),
            "fresh_ratio": round(fresh_ratio, 4),
        },
    }


def _evaluate_benchmark_freshness(fresh_days: int = 5) -> dict:
    benchmark_codes = ["510300", "510500", "159915"]
    bad = []
    for code in benchmark_codes:
        f = DATA / f"etf_{code}.csv"
        if not f.exists():
            bad.append(f"{code}:missing")
            continue
        try:
            df = pd.read_csv(f, usecols=["date"])
            latest = pd.to_datetime(df["date"], errors="coerce").max()
            if pd.isna(latest) or latest < (pd.Timestamp.today().normalize() - pd.Timedelta(days=fresh_days)):
                bad.append(f"{code}:stale")
        except Exception:
            bad.append(f"{code}:read_error")

    if bad:
        return {"status": "FAIL", "reason": "benchmark_not_fresh", "stats": {"issues": bad}}
    return {"status": "PASS", "reason": "", "stats": {"issues": []}}


def _merge_status(parts: list[dict]) -> str:
    states = [p.get("status", "PASS") for p in parts]
    if "FAIL" in states:
        return "FAIL"
    if "WARN" in states:
        return "WARN"
    return "PASS"


def _render_markdown(final_status: str, parts: dict) -> str:
    lines = [
        "# 模拟前检查（Preflight）\n",
        f"- 总状态: **{final_status}**",
        "- 说明: PASS可继续，WARN建议谨慎，FAIL建议先排查后再跑主流程。\n",
    ]
    for name, payload in parts.items():
        lines.append(f"## {name}")
        lines.append(f"- status: {payload.get('status')}")
        lines.append(f"- reason: {payload.get('reason', '')}")
        lines.append(f"- stats: {payload.get('stats', {})}\n")
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Run preflight checks before paper simulation")
    parser.add_argument("--strict", action="store_true", help="Exit non-zero on FAIL")
    parser.add_argument("--push", action="store_true", help="Push preflight result to Feishu")
    args = parser.parse_args()

    cfg = _load_config()
    pre_cfg = (((cfg.get("operations") or {}).get("preflight") or {}) if isinstance(cfg, dict) else {})
    fresh_days = int(pre_cfg.get("fresh_days", 5))
    min_rows = int(pre_cfg.get("min_rows", 240))
    min_usable_ratio_fail = float(pre_cfg.get("min_usable_ratio_fail", 0.03))
    min_usable_ratio_warn = float(pre_cfg.get("min_usable_ratio_warn", 0.10))
    min_usable_count_fail = int(pre_cfg.get("min_usable_count_fail", 5))

    fetch_df = _safe_read_csv(REPORTS / "paper_rotation_fetch_status.csv")

    parts = {
        "fetch_status": _evaluate_fetch_status(fetch_df),
        "cache_coverage": _evaluate_cache_coverage(
            fresh_days=fresh_days,
            min_rows=min_rows,
            min_usable_ratio_fail=min_usable_ratio_fail,
            min_usable_ratio_warn=min_usable_ratio_warn,
            min_usable_count_fail=min_usable_count_fail,
        ),
        "benchmark_freshness": _evaluate_benchmark_freshness(fresh_days=fresh_days),
    }

    final_status = _merge_status(list(parts.values()))
    payload = {"status": final_status, "parts": parts}

    REPORTS.mkdir(parents=True, exist_ok=True)
    json_path = REPORTS / "paper_rotation_preflight.json"
    md_path = REPORTS / "paper_rotation_preflight.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(_render_markdown(final_status, parts), encoding="utf-8")

    text = f"Preflight={final_status} | json={json_path} | md={md_path}"
    print(text)

    if args.push:
        try:
            push_dm(text)
        except Exception as e:
            print(f"[warn] preflight feishu push failed: {e}")

    if args.strict and final_status == "FAIL":
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
