from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / "reports"
LOGS = ROOT / "logs"
STATE_PATH = REPORTS / "daily_runner_state.json"
STOP_FLAG = REPORTS / "STOP_DAILY_RUNNER"
LOCK_PATH = REPORTS / "daily_runner.lock"


@dataclass
class RunnerState:
    last_run_at: str = ""
    last_status: str = ""
    last_code: int = 0
    total_runs: int = 0


def _load_state() -> RunnerState:
    if not STATE_PATH.exists():
        return RunnerState()
    try:
        payload = json.loads(STATE_PATH.read_text(encoding="utf-8"))
        return RunnerState(
            last_run_at=str(payload.get("last_run_at", "")),
            last_status=str(payload.get("last_status", "")),
            last_code=int(payload.get("last_code", 0)),
            total_runs=int(payload.get("total_runs", 0)),
        )
    except Exception:
        return RunnerState()


def _save_state(state: RunnerState) -> None:
    REPORTS.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(asdict(state), ensure_ascii=False, indent=2), encoding="utf-8")


def _log_line(message: str) -> None:
    LOGS.mkdir(parents=True, exist_ok=True)
    log_path = LOGS / "daily_runner.log"
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {message}"
    print(line, flush=True)
    with log_path.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except Exception:
        return False


def _acquire_lock() -> bool:
    REPORTS.mkdir(parents=True, exist_ok=True)
    if LOCK_PATH.exists():
        try:
            payload = json.loads(LOCK_PATH.read_text(encoding="utf-8"))
            pid = int(payload.get("pid", 0))
            if _pid_alive(pid):
                _log_line(f"Another runner is already running (pid={pid}), exit.")
                return False
        except Exception:
            pass

    payload = {"pid": os.getpid(), "started_at": datetime.now().isoformat()}
    LOCK_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return True


def _release_lock() -> None:
    try:
        if LOCK_PATH.exists():
            LOCK_PATH.unlink(missing_ok=True)
    except Exception:
        pass


def _parse_time(time_str: str) -> tuple[int, int]:
    hh, mm = time_str.split(":", 1)
    h = int(hh)
    m = int(mm)
    if not (0 <= h <= 23 and 0 <= m <= 59):
        raise ValueError("time must be HH:MM")
    return h, m


def _next_run(now: datetime, target_h: int, target_m: int) -> datetime:
    candidate = now.replace(hour=target_h, minute=target_m, second=0, microsecond=0)
    if candidate <= now:
        candidate = candidate + timedelta(days=1)
    return candidate


def _run_daily_pipeline(python_exe: str) -> int:
    cmd = [python_exe, "scripts/run_daily_pipeline.py"]
    _log_line(f"Run command: {' '.join(cmd)}")
    proc = subprocess.run(cmd, cwd=str(ROOT), capture_output=True, text=True)
    if proc.stdout:
        _log_line("[stdout]\n" + proc.stdout.rstrip())
    if proc.stderr:
        _log_line("[stderr]\n" + proc.stderr.rstrip())
    _log_line(f"Exit code: {proc.returncode}")
    return int(proc.returncode)


def main() -> int:
    parser = argparse.ArgumentParser(description="Long-running daily scheduler for run_daily_pipeline.py")
    parser.add_argument("--time", default="18:30", help="Daily run time HH:MM")
    parser.add_argument("--run-on-start", action="store_true", help="Run once immediately after start")
    parser.add_argument("--no-loop", action="store_true", help="Run once and exit (for tests)")
    parser.add_argument("--python-exe", default=sys.executable, help="Python executable path")
    args = parser.parse_args()

    target_h, target_m = _parse_time(args.time)
    state = _load_state()

    if not _acquire_lock():
        return 1

    _log_line("Daily runner started")
    _log_line(f"Target time: {args.time}")

    if STOP_FLAG.exists():
        STOP_FLAG.unlink(missing_ok=True)

    try:
        if args.run_on_start:
            code = _run_daily_pipeline(args.python_exe)
            state.total_runs += 1
            state.last_run_at = datetime.now().isoformat()
            state.last_code = code
            state.last_status = "ok" if code == 0 else "failed"
            _save_state(state)
            if args.no_loop:
                return code

        if args.no_loop:
            _log_line("No-loop mode enabled, exit")
            return 0

        last_trigger_date = ""
        while True:
            if STOP_FLAG.exists():
                _log_line(f"Stop flag detected: {STOP_FLAG}")
                STOP_FLAG.unlink(missing_ok=True)
                break

            now = datetime.now()
            trigger = now.replace(hour=target_h, minute=target_m, second=0, microsecond=0)
            today = now.strftime("%Y-%m-%d")

            if now >= trigger and last_trigger_date != today:
                code = _run_daily_pipeline(args.python_exe)
                state.total_runs += 1
                state.last_run_at = datetime.now().isoformat()
                state.last_code = code
                state.last_status = "ok" if code == 0 else "failed"
                _save_state(state)
                last_trigger_date = today

            nxt = _next_run(datetime.now(), target_h, target_m)
            wait_sec = int((nxt - datetime.now()).total_seconds())
            _log_line(f"Next run at {nxt.strftime('%Y-%m-%d %H:%M:%S')} (in {wait_sec}s)")
            time.sleep(min(60, max(5, wait_sec)))

        _log_line("Daily runner stopped")
        return 0
    finally:
        _release_lock()


if __name__ == "__main__":
    raise SystemExit(main())
