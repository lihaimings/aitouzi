import json
import argparse
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.reporting.feishu_push import push_dm

REPORTS = ROOT / "reports"
STATE_PATH = REPORTS / "system_health_state.json"


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


def _load_state() -> dict:
    if not STATE_PATH.exists():
        return {"red_streak": 0, "last_status": "UNKNOWN", "last_alert_streak": 0}
    try:
        d = json.loads(STATE_PATH.read_text(encoding="utf-8"))
        return d if isinstance(d, dict) else {"red_streak": 0, "last_status": "UNKNOWN", "last_alert_streak": 0}
    except Exception:
        return {"red_streak": 0, "last_status": "UNKNOWN", "last_alert_streak": 0}


def _save_state(state: dict) -> None:
    REPORTS.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


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
    parser = argparse.ArgumentParser(description="Build and optionally push system health")
    parser.add_argument("--weekly", action="store_true", help="Force weekly summary push")
    args = parser.parse_args()

    cfg = _load_config()
    health_cfg = (((cfg.get("operations") or {}).get("health_alert") or {}) if isinstance(cfg, dict) else {})
    red_days = int(health_cfg.get("consecutive_red_days", 2))

    fetch_status = _safe_read_csv(REPORTS / "paper_rotation_fetch_status.csv")
    fetch_history = _safe_read_csv(REPORTS / "paper_rotation_fetch_history.csv")
    preflight = _safe_read_json(REPORTS / "paper_rotation_preflight.json")

    total = int(len(fetch_status)) if not fetch_status.empty else 0
    counts = fetch_status["status"].value_counts().to_dict() if (not fetch_status.empty and "status" in fetch_status.columns) else {}
    ok = int(counts.get("ok", 0))
    short = int(counts.get("short", 0))
    stale = int(counts.get("stale", 0))
    failed = int(counts.get("failed", 0))
    pending_retry = int(counts.get("待补全", 0))
    queued = int(counts.get("queued", 0))
    success_ratio = float((ok + short) / total) if total > 0 else 0.0
    failed_ratio = float((failed + pending_retry) / total) if total > 0 else 0.0

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
    if preflight_status == "FAIL" or failed_ratio > 0.05:
        health_status = "RED"
    elif preflight_status == "WARN" or stale > 0 or queued > 0:
        health_status = "YELLOW"

    payload = {
        "status": health_status,
        "preflight": preflight_status,
        "fetch": {
            "total": total,
            "ok": ok,
            "short": short,
            "stale": stale,
            "failed": failed,
            "pending_retry": pending_retry,
            "failed_ratio": round(failed_ratio, 4),
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

    state = _load_state()
    red_streak = int(state.get("red_streak", 0))
    if health_status == "RED":
        red_streak += 1
    else:
        red_streak = 0
        state["last_alert_streak"] = 0
    payload["consecutive_red_days"] = red_streak

    REPORTS.mkdir(parents=True, exist_ok=True)
    json_path = REPORTS / "system_health.json"
    md_path = REPORTS / "system_health.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    md = (
        "# System Health\n\n"
        f"- status: **{health_status}**\n"
        f"- preflight: {preflight_status}\n"
        f"- fetch: total={total}, ok={ok}, short={short}, stale={stale}, failed={failed}, pending_retry={pending_retry}, queued={queued}, success_ratio={success_ratio:.2%}\n"
        f"- 30d: runs={run_30d}, failed_total={error_30d}, avg_duration={avg_duration:.1f}s, p95_duration={p95_duration:.1f}s\n"
    )
    md_path.write_text(md, encoding="utf-8")

    zh = {"GREEN": "绿色", "YELLOW": "黄色", "RED": "红色"}
    pre_zh = {"PASS": "通过", "WARN": "警告", "FAIL": "未通过"}
    summary = (
        f"系统健康={zh.get(health_status, health_status)} | "
        f"模拟前检查={pre_zh.get(preflight_status, preflight_status)} | "
        f"成功+短历史={ok+short}/{total} | 失败={failed} | 待补全={pending_retry} | 排队待补抓={queued}"
    )
    print(summary)

    should_push = False
    if args.weekly:
        should_push = True
    elif health_status == "RED" and red_streak >= max(1, red_days):
        last_alert_streak = int(state.get("last_alert_streak", 0))
        if red_streak > last_alert_streak:
            should_push = True
            state["last_alert_streak"] = red_streak

    state["red_streak"] = red_streak
    state["last_status"] = health_status
    _save_state(state)

    if should_push:
        alert_text = (
            f"系统健康告警: 连续{red_streak}天{zh.get(health_status, health_status)}，建议暂停观察\n"
            f"模拟前检查={pre_zh.get(preflight_status, preflight_status)}, 抓数: 成功+短历史={ok+short}/{total}, 失败={failed}, 待补全={pending_retry}, 排队待补抓={queued}"
        )
        try:
            push_dm(alert_text)
        except Exception as e:
            print(f"[warn] health push failed: {e}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
