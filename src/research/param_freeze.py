from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict

import pandas as pd


@dataclass
class FreezeDecision:
    allowed: bool
    reason: str
    days_since_last_apply: int
    freeze_days: int


def _stable_hash(payload: Dict) -> str:
    text = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def evaluate_param_freeze(
    candidate_params: Dict,
    state_path: Path,
    freeze_days: int = 90,
    force: bool = False,
) -> FreezeDecision:
    if force:
        return FreezeDecision(True, "force_override", 9999, freeze_days)

    if not state_path.exists():
        return FreezeDecision(True, "first_apply", 9999, freeze_days)

    try:
        state = json.loads(state_path.read_text(encoding="utf-8"))
    except Exception:
        return FreezeDecision(True, "invalid_state_recover", 9999, freeze_days)

    last_time = pd.to_datetime(state.get("last_applied_at"), errors="coerce")
    if pd.isna(last_time):
        return FreezeDecision(True, "missing_last_applied_time", 9999, freeze_days)

    now = pd.Timestamp.now()
    days_since = int((now - last_time).days)
    candidate_hash = _stable_hash(candidate_params)
    last_hash = str(state.get("params_hash", ""))

    if candidate_hash == last_hash:
        return FreezeDecision(True, "same_params_no_change", days_since, freeze_days)

    if days_since < freeze_days:
        return FreezeDecision(False, "freeze_window_not_passed", days_since, freeze_days)

    return FreezeDecision(True, "freeze_window_passed", days_since, freeze_days)


def persist_param_freeze_state(candidate_params: Dict, state_path: Path) -> Path:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "last_applied_at": pd.Timestamp.now().isoformat(),
        "params_hash": _stable_hash(candidate_params),
        "params": candidate_params,
    }
    state_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return state_path
