import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.reporting.feishu_push import push_dm

REPORTS = ROOT / "reports"


def _safe_read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def _safe_read_json(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def main() -> int:
    fetch_status = _safe_read_csv(REPORTS / "paper_rotation_fetch_status.csv")
    fetch_history = _safe_read_csv(REPORTS / "paper_rotation_fetch_history.csv")
    preflight = _safe_read_json(REPORTS / "paper_rotation_preflight.json")

    total = int(len(fetch_status)) if not fetch_status.empty else 0
    counts = fetch_status["status"].value_counts().to_dict() if (not fetch_status.empty and "status" in fetch_status.columns) else {}
    ok = int(counts.get("ok", 0))
    stale = int(counts.get("stale", 0))
    failed = int(counts.get("failed", 0))
    queued = int(counts.get("queued", 0))
    success_ratio = float(ok / total) if total > 0 else 0.0

    avg_duration = 0.0
    p95_duration = 0.0
    error_30d = 0
    run_30d = 0
    if not fetch_history.empty:
        fetch_history["run_at"] = pd.to_datetime(fetch_history.get("run_at"), errors="coerce")
        cutoff = pd.Timestamp.now() - pd.Timedelta(days=30)
        last_30d = fetch_history[fetch_history["run_at"] >= cutoff]
        run_30d = int(len(last_30d))
        if "duration_sec" in last_30d.columns and run_30d > 0:
            avg_duration = float(pd.to_numeric(last_30d["duration_sec"], errors="coerce").mean())
            p95_duration = float(pd.to_numeric(last_30d["duration_sec"], errors="coerce").quantile(0.95))
        if "failed" in last_30d.columns:
            error_30d = int(pd.to_numeric(last_30d["failed"], errors="coerce").fillna(0).sum())

    preflight_status = str(preflight.get("status", "UNKNOWN"))

    health_status = "GREEN"
    if preflight_status == "FAIL" or failed > 0:
        health_status = "RED"
    elif preflight_status == "WARN" or stale > 0 or queued > 0:
        health_status = "YELLOW"

    payload = {
        "status": health_status,
        "preflight": preflight_status,
        "fetch": {
            "total": total,
            "ok": ok,
            "stale": stale,
            "failed": failed,
            "queued": queued,
            "success_ratio": round(success_ratio, 4),
        },
        "fetch_30d": {
            "runs": run_30d,
            "failed_total": error_30d,
            "avg_duration_sec": round(avg_duration, 3),
            "p95_duration_sec": round(p95_duration, 3),
        },
    }

    REPORTS.mkdir(parents=True, exist_ok=True)
    json_path = REPORTS / "system_health.json"
    md_path = REPORTS / "system_health.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    md = (
        "# System Health\n\n"
        f"- status: **{health_status}**\n"
        f"- preflight: {preflight_status}\n"
        f"- fetch: total={total}, ok={ok}, stale={stale}, failed={failed}, queued={queued}, success_ratio={success_ratio:.2%}\n"
        f"- 30d: runs={run_30d}, failed_total={error_30d}, avg_duration={avg_duration:.1f}s, p95_duration={p95_duration:.1f}s\n"
    )
    md_path.write_text(md, encoding="utf-8")

    summary = f"SystemHealth={health_status} | preflight={preflight_status} | ok={ok}/{total} | failed={failed} | queued={queued}"
    print(summary)
    try:
        push_dm(summary)
    except Exception as e:
        print(f"[warn] health push failed: {e}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
