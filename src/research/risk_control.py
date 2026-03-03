from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

REPORT_DIR = Path(__file__).resolve().parents[2] / "reports"


@dataclass
class RiskLimits:
    max_daily_drawdown: float = -0.05
    max_total_drawdown: float = -0.15
    max_position_weight: float = 0.50


def evaluate_risk_guardrails(
    metrics: Dict[str, float],
    daily_returns: pd.Series,
    weights: pd.DataFrame,
    limits: RiskLimits,
) -> Dict:
    daily_returns = pd.Series(daily_returns).fillna(0.0)
    weights = pd.DataFrame(weights).fillna(0.0)

    observed_max_daily = float(daily_returns.min()) if len(daily_returns) > 0 else 0.0
    observed_max_dd = float(metrics.get("max_drawdown", 0.0))
    observed_max_weight = float(weights.max().max()) if not weights.empty else 0.0

    checks = {
        "max_daily_drawdown": {
            "limit": limits.max_daily_drawdown,
            "observed": observed_max_daily,
            "pass": observed_max_daily >= limits.max_daily_drawdown,
        },
        "max_total_drawdown": {
            "limit": limits.max_total_drawdown,
            "observed": observed_max_dd,
            "pass": observed_max_dd >= limits.max_total_drawdown,
        },
        "max_position_weight": {
            "limit": limits.max_position_weight,
            "observed": observed_max_weight,
            "pass": observed_max_weight <= limits.max_position_weight,
        },
    }

    fail_items: List[str] = [k for k, v in checks.items() if not bool(v["pass"])]
    status = "PASS" if not fail_items else "FAIL"

    return {
        "status": status,
        "limits": asdict(limits),
        "checks": checks,
        "fail_items": fail_items,
        "note": "风控阈值仅用于预警与人工审核，不自动停用策略参数。",
    }


def _render_guardrail_markdown(review: Dict) -> str:
    lines = [
        "# 风控阈值检查（自动生成）\n",
        f"- 状态: **{review.get('status', 'PASS')}**",
        f"- 失败项: {review.get('fail_items', [])}\n",
        "## 检查明细",
        "| check | observed | limit | pass |",
        "|---|---:|---:|---|",
    ]

    checks = review.get("checks", {}) or {}
    for name, payload in checks.items():
        obs = float(payload.get("observed", 0.0))
        lim = float(payload.get("limit", 0.0))
        flag = bool(payload.get("pass", False))
        lines.append(f"| {name} | {obs:.4f} | {lim:.4f} | {'PASS' if flag else 'FAIL'} |")

    lines.extend(
        [
            "\n## 审核建议",
            "- 若状态为 FAIL，建议先人工复核数据质量与回测窗口，再决定是否调整参数。",
            "- 风控阈值建议在一段时间内保持稳定，避免频繁修改造成过拟合。",
        ]
    )
    return "\n".join(lines) + "\n"


def save_risk_guardrails_review(
    review: Dict,
    prefix: str = "paper_rotation",
) -> Tuple[Path, Path]:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    json_path = REPORT_DIR / f"{prefix}_risk_guardrails.json"
    md_path = REPORT_DIR / f"{prefix}_risk_guardrails.md"

    json_path.write_text(json.dumps(review, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(_render_guardrail_markdown(review), encoding="utf-8")
    return json_path, md_path
