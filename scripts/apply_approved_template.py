from pathlib import Path
import shutil
import json
import argparse
import sys

import yaml  # type: ignore

ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / "reports"
FREEZE_STATE = REPORTS / "paper_rotation_param_freeze_state.json"

sys.path.insert(0, str(ROOT))

from src.research import evaluate_param_freeze, persist_param_freeze_state


def _load_config() -> dict:
    cfg_path = ROOT / "config.yaml"
    if not cfg_path.exists():
        return {}
    try:
        d = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}


def main():
    parser = argparse.ArgumentParser(description="Apply approved params template with freeze guard")
    parser.add_argument("--force", action="store_true", help="Bypass freeze window check")
    args = parser.parse_args()

    template = REPORTS / "paper_rotation_approved_params_template.json"
    approved = REPORTS / "paper_rotation_approved_params.json"

    if not template.exists():
        print(f"[warn] template not found: {template}")
        return

    candidate = json.loads(template.read_text(encoding="utf-8"))

    cfg = _load_config()
    freeze_cfg = (((cfg.get("operations") or {}).get("param_freeze") or {}) if isinstance(cfg, dict) else {})
    freeze_days = int(freeze_cfg.get("freeze_days", 90))

    decision = evaluate_param_freeze(
        candidate_params=candidate,
        state_path=FREEZE_STATE,
        freeze_days=freeze_days,
        force=bool(args.force),
    )
    if not decision.allowed:
        print(
            "[block] parameter apply denied by freeze policy: "
            f"reason={decision.reason}, days_since_last={decision.days_since_last_apply}, freeze_days={decision.freeze_days}"
        )
        return

    backup = None
    if approved.exists():
        backup = REPORTS / "paper_rotation_approved_params.backup.json"
        shutil.copyfile(approved, backup)

    shutil.copyfile(template, approved)
    persist_param_freeze_state(candidate_params=candidate, state_path=FREEZE_STATE)
    print(f"applied: {approved}")
    print(
        f"freeze_decision: reason={decision.reason}, "
        f"days_since_last={decision.days_since_last_apply}, freeze_days={decision.freeze_days}"
    )
    if backup is not None:
        print(f"backup: {backup}")


if __name__ == "__main__":
    main()
