from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, Iterable

import json

try:
    import yaml  # type: ignore
except Exception:  # pragma: no cover
    yaml = None

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG = ROOT / "config" / "strategy_gatekeeper.yaml"


@dataclass
class GatekeeperResult:
    state: str
    score: float
    actions: Dict[str, Any]
    metrics: Dict[str, float]
    reasons: Dict[str, float]
    thresholds: Dict[str, float]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def to_markdown(self) -> str:
        metrics_md = "\n".join(f"- `{k}`: {v:.4f}" for k, v in self.metrics.items())
        reasons_md = "\n".join(f"- `{k}`: {v:.4f}" for k, v in self.reasons.items())
        actions_md = "\n".join(f"- `{k}`: {v}" for k, v in self.actions.items()) or "- 无"
        return (
            "# 红黄绿灯总闸门快照\n\n"
            f"- 状态：`{self.state}`\n"
            f"- 评分：`{self.score:.4f}`\n"
            f"- 阈值：green<={self.thresholds.get('green_max', 0.33):.2f}, yellow<={self.thresholds.get('yellow_max', 0.66):.2f}, else red\n\n"
            "## 指标归一化\n"
            f"{metrics_md}\n\n"
            "## 加权贡献\n"
            f"{reasons_md}\n\n"
            "## 动作约束\n"
            f"{actions_md}\n"
        )


def load_gatekeeper_config(path: Path | None = None) -> Dict[str, Any]:
    cfg_path = path or DEFAULT_CONFIG
    if not cfg_path.exists() or yaml is None:
        return {}
    data = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))
    return data if isinstance(data, dict) else {}


def _normalize_metric(value: float | None, fill: float = 0.5) -> float:
    if value is None:
        return float(fill)
    return max(0.0, min(1.0, float(value)))


def _extract_metric(metrics: Dict[str, Any], key: str, aliases: Iterable[str] = ()) -> float | None:
    keys = (key, *aliases)
    for candidate in keys:
        if candidate in metrics and metrics[candidate] is not None:
            return float(metrics[candidate])
    return None


def build_gatekeeper_metrics(raw_metrics: Dict[str, Any]) -> Dict[str, float | None]:
    return {
        "macro_risk": _extract_metric(raw_metrics, "macro_risk", aliases=("macro", "macro_score")),
        "drawdown_risk": _extract_metric(raw_metrics, "drawdown_risk", aliases=("drawdown", "max_drawdown_risk")),
        "breadth_risk": _extract_metric(raw_metrics, "breadth_risk", aliases=("breadth", "market_breadth_risk")),
        "volatility_risk": _extract_metric(raw_metrics, "volatility_risk", aliases=("volatility", "vol_risk")),
    }


def score_gatekeeper(metrics: Dict[str, float | None], config: Dict[str, Any] | None = None) -> GatekeeperResult:
    cfg = config or load_gatekeeper_config()
    gate_cfg = cfg.get("gatekeeper", {}) if isinstance(cfg, dict) else {}
    weights = gate_cfg.get("score_weights", {}) or {}
    thresholds = gate_cfg.get("thresholds", {}) or {}
    actions = gate_cfg.get("actions", {}) or {}
    defaults = gate_cfg.get("defaults", {}) or {}
    fill = float(defaults.get("missing_metric_fill", 0.5))

    normalized = {
        key: _normalize_metric(metrics.get(key), fill)
        for key in ["macro_risk", "drawdown_risk", "breadth_risk", "volatility_risk"]
    }
    weighted = {k: normalized[k] * float(weights.get(k, 0.25)) for k in normalized}
    score = round(sum(weighted.values()), 4)

    green_max = float(thresholds.get("green_max", 0.33))
    yellow_max = float(thresholds.get("yellow_max", 0.66))
    if score <= green_max:
        state = "green"
    elif score <= yellow_max:
        state = "yellow"
    else:
        state = "red"

    return GatekeeperResult(
        state=state,
        score=score,
        actions=actions.get(state, {}),
        metrics={k: float(v) for k, v in normalized.items()},
        reasons={k: round(v, 4) for k, v in weighted.items()},
        thresholds={"green_max": green_max, "yellow_max": yellow_max},
    )


def save_gatekeeper_snapshot(result: GatekeeperResult, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(result.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")


def save_gatekeeper_markdown(result: GatekeeperResult, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(result.to_markdown(), encoding="utf-8")
