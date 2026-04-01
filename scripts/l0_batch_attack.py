#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
import time
from collections import Counter
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BATCH_DIR = ROOT / "state" / "l0_batches"
REPORT_JSON = ROOT / "state" / "l0_attack_progress.json"
STATUS_JSON = ROOT / "reports" / "paper_rotation_fetch_status.json"


def run_fetch(codes_file: Path, limit: int, source_order: str, retries: int, repair_rounds: int) -> dict:
    cmd = [
        sys.executable,
        "scripts/fetch_etf_cache.py",
        "--codes-file",
        str(codes_file),
        "--source-order",
        source_order,
        "--max-retries",
        str(retries),
        "--retry-sleep",
        "0.3",
        "--fresh-tolerance-days",
        "3",
        "--backfill-days",
        "1800",
        "--min-bootstrap-rows",
        "240",
        "--repair-rounds",
        str(repair_rounds),
        "--crawler-only",
        "0",
    ]
    if limit > 0:
        cmd += ["--limit", str(limit)]

    r = subprocess.run(cmd, cwd=str(ROOT), capture_output=True, text=True)
    out = (r.stdout or "") + "\n" + (r.stderr or "")

    status = []
    if STATUS_JSON.exists():
        try:
            status = json.loads(STATUS_JSON.read_text(encoding="utf-8"))
        except Exception:
            status = []

    c = Counter()
    for row in status if isinstance(status, list) else []:
        c[row.get("status", "unknown")] += 1

    return {
        "ok": c.get("ok", 0),
        "short": c.get("short", 0) + c.get("short_history", 0),
        "stale": c.get("stale", 0),
        "fail": c.get("failed", 0) + c.get("fail", 0),
        "returncode": r.returncode,
        "log_tail": "\n".join(out.splitlines()[-40:]),
    }


def probe(batch_file: Path, probe_size: int) -> dict:
    results = {}
    for src in ["ths", "eastmoney", "tencent", "sina", "netease", "akshare"]:
        results[src] = run_fetch(batch_file, probe_size, src, retries=1, repair_rounds=0)
    return results


def append_csv(path: Path, row: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not path.exists()
    with path.open("a", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(row.keys()))
        if write_header:
            w.writeheader()
        w.writerow(row)


def main() -> int:
    ap = argparse.ArgumentParser(description="Attack L0 historical fetch by 200-batch with source probe")
    ap.add_argument("--start", type=int, default=1)
    ap.add_argument("--end", type=int, default=8)
    ap.add_argument("--probe-size", type=int, default=10)
    ap.add_argument("--sleep-seconds", type=int, default=900)
    ap.add_argument("--max-rounds", type=int, default=6)
    args = ap.parse_args()

    progress = {
        "updated_at": datetime.now().isoformat(timespec="seconds"),
        "start": args.start,
        "end": args.end,
        "rounds": [],
    }

    for b in range(args.start, args.end + 1):
        batch_file = BATCH_DIR / f"batch_{b:02d}.csv"
        if not batch_file.exists():
            continue

        solved = False
        for r in range(1, args.max_rounds + 1):
            p = probe(batch_file, args.probe_size)
            best_src = max(p.keys(), key=lambda k: p[k]["ok"])
            best_ok = p[best_src]["ok"]

            row = {
                "ts": datetime.now().isoformat(timespec="seconds"),
                "batch": b,
                "round": r,
                "probe_ok_ths": p["ths"]["ok"],
                "probe_ok_eastmoney": p["eastmoney"]["ok"],
                "probe_ok_tencent": p["tencent"]["ok"],
                "probe_ok_sina": p["sina"]["ok"],
                "probe_ok_netease": p["netease"]["ok"],
                "probe_ok_akshare": p["akshare"]["ok"],
                "best_source": best_src,
                "best_ok": best_ok,
            }
            append_csv(ROOT / "state" / "l0_attack_probe_log.csv", row)

            if best_ok > 0:
                full = run_fetch(batch_file, 0, best_src, retries=2, repair_rounds=1)
                row2 = {
                    "ts": datetime.now().isoformat(timespec="seconds"),
                    "batch": b,
                    "round": r,
                    "mode": "full_run",
                    "source": best_src,
                    "ok": full["ok"],
                    "short": full["short"],
                    "stale": full["stale"],
                    "fail": full["fail"],
                    "returncode": full["returncode"],
                }
                append_csv(ROOT / "state" / "l0_attack_full_log.csv", row2)
                progress["rounds"].append({"batch": b, "round": r, "probe": p, "full": full, "source": best_src})
                solved = True
                break

            progress["rounds"].append({"batch": b, "round": r, "probe": p, "full": None, "source": None})
            if r < args.max_rounds:
                time.sleep(max(1, args.sleep_seconds))

        if not solved:
            # continue next batch in next manual run
            break

    progress["updated_at"] = datetime.now().isoformat(timespec="seconds")
    REPORT_JSON.write_text(json.dumps(progress, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[ok] progress written: {REPORT_JSON}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
