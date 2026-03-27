import argparse
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
STATE_PATH = ROOT / "data" / "layers" / "layer_fetch_health_state.json"


def _load_state() -> dict:
    if not STATE_PATH.exists():
        return {"layers": {}}
    try:
        data = json.loads(STATE_PATH.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {"layers": {}}
    except Exception:
        return {"layers": {}}


def _save_state(state: dict) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def _run(cmd: list[str]) -> tuple[int, str, str]:
    print(f"[step] {' '.join(cmd)}")
    r = subprocess.run(cmd, cwd=str(ROOT), capture_output=True, text=True)
    if r.stdout:
        print(r.stdout)
    if r.stderr:
        print(r.stderr)
    return int(r.returncode), str(r.stdout or ""), str(r.stderr or "")


def _fetch_cmd(py: str, layer_name: str, codes_file: str, layer_limit: int = 0, degraded: bool = False) -> list[str]:
    # layer-specific defaults (aligned with consensus doc)
    if layer_name == "l0":
        args = [
            "--codes-file", codes_file,
            "--source-order", "ths,eastmoney,akshare",
            "--max-retries", "2",
            "--fresh-tolerance-days", "2",
            "--backfill-days", "1800",
            "--min-bootstrap-rows", "240",
            "--repair-rounds", "1",
        ]
    elif layer_name == "l1":
        args = [
            "--codes-file", codes_file,
            "--source-order", "ths,eastmoney,akshare",
            "--max-retries", "3",
            "--fresh-tolerance-days", "1",
            "--backfill-days", "1500",
            "--min-bootstrap-rows", "500",
            "--repair-rounds", "2",
        ]
    else:  # l2
        args = [
            "--codes-file", codes_file,
            "--source-order", "ths,eastmoney,akshare",
            "--max-retries", "3",
            "--fresh-tolerance-days", "0",
            "--backfill-days", "365",
            "--min-bootstrap-rows", "500",
            "--repair-rounds", "2",
        ]

    if degraded and layer_name == "l2":
        # degraded mode: lower intensity on repeated failures
        args += ["--repair-rounds", "1", "--max-retries", "2"]

    cmd = [py, "scripts/fetch_etf_cache.py", "--crawler-only", "0", "--strict"] + args
    if layer_limit > 0:
        cmd += ["--limit", str(layer_limit)]
    return cmd


def _update_layer_state(state: dict, layer: str, ok: bool, note: str = "") -> dict:
    layers = state.setdefault("layers", {})
    row = layers.setdefault(layer, {
        "consecutive_failures": 0,
        "status": "ok",
        "degraded": False,
        "paused": False,
        "last_ok_at": None,
        "last_error_at": None,
        "last_note": "",
    })

    now = datetime.now().isoformat(timespec="seconds")
    if ok:
        row["consecutive_failures"] = 0
        row["status"] = "ok"
        row["degraded"] = False
        row["paused"] = False
        row["last_ok_at"] = now
        row["last_note"] = "ok"
        return state

    # failure path
    row["consecutive_failures"] = int(row.get("consecutive_failures", 0) or 0) + 1
    n = row["consecutive_failures"]
    row["last_error_at"] = now
    row["last_note"] = note[:500]

    # thresholds from consensus: 2 warn, 3 degraded, 5 switch backup, 8 pause
    if n >= 8:
        row["status"] = "paused"
        row["paused"] = True
    elif n >= 5:
        row["status"] = "backup_source"
        row["degraded"] = True
    elif n >= 3:
        row["status"] = "degraded"
        row["degraded"] = True
    elif n >= 2:
        row["status"] = "warn"
    else:
        row["status"] = "error"

    return state


def main() -> int:
    parser = argparse.ArgumentParser(description="Run layered ETF fetch (L0/L1/L2)")
    parser.add_argument("--mode", choices=["full", "l0", "l1", "l2"], default="full")
    parser.add_argument("--l1-size", type=int, default=200)
    parser.add_argument("--l0-limit", type=int, default=0)
    parser.add_argument("--l1-limit", type=int, default=0)
    parser.add_argument("--l2-limit", type=int, default=0)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    py = sys.executable
    state = _load_state()

    bootstrap_steps: list[list[str]] = [
        [py, "scripts/refresh_etf_universe.py", "--size", str(max(20, args.l1_size))],
        [py, "scripts/build_layer_universe.py", "--l1-size", str(max(20, args.l1_size))],
    ]

    layers_dir = ROOT / "data" / "layers"
    layer_files = {
        "l0": str(layers_dir / "l0_all_codes.csv"),
        "l1": str(layers_dir / "l1_watch_codes.csv"),
        "l2": str(layers_dir / "l2_core_codes.csv"),
    }

    fetch_plan: list[tuple[str, list[str]]] = []
    if args.mode == "full":
        for name, lim in [("l0", args.l0_limit), ("l1", args.l1_limit), ("l2", args.l2_limit)]:
            row = (state.get("layers", {}) or {}).get(name, {})
            if bool(row.get("paused", False)):
                print(f"[skip] {name} paused by health state (>=8 consecutive failures)")
                continue
            degraded = bool(row.get("degraded", False))
            fetch_plan.append((name, _fetch_cmd(py, name, layer_files[name], layer_limit=max(0, lim), degraded=degraded)))
    else:
        row = (state.get("layers", {}) or {}).get(args.mode, {})
        if bool(row.get("paused", False)):
            print(f"[skip] {args.mode} paused by health state (>=8 consecutive failures)")
            _save_state(state)
            return 0
        lim = max(0, getattr(args, f"{args.mode}_limit"))
        degraded = bool(row.get("degraded", False))
        fetch_plan.append((args.mode, _fetch_cmd(py, args.mode, layer_files[args.mode], layer_limit=lim, degraded=degraded)))

    # run bootstrap always
    for cmd in bootstrap_steps:
        if args.dry_run:
            print("[dry-run]", " ".join(cmd))
        else:
            code, out, err = _run(cmd)
            if code != 0:
                raise RuntimeError(f"bootstrap step failed: {' '.join(cmd)}")

    failed = 0
    for layer_name, cmd in fetch_plan:
        if args.dry_run:
            print("[dry-run]", " ".join(cmd))
            continue

        code, out, err = _run(cmd)
        ok = code == 0
        note = (err or out or "").strip()
        state = _update_layer_state(state, layer_name, ok=ok, note=note)
        if not ok:
            failed += 1

    _save_state(state)
    print(f"[state] {STATE_PATH}")
    print("[done] layered fetch finished")

    return 2 if failed > 0 else 0


if __name__ == "__main__":
    raise SystemExit(main())
