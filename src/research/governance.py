from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Tuple

import pandas as pd

REPORT_DIR = Path(__file__).resolve().parents[2] / "reports"


def build_research_recommendation(
    stability_df: pd.DataFrame,
    wf_table: pd.DataFrame,
    current_params: Dict[str, float],
    min_pass_ratio: float = 0.6,
    min_avg_sharpe: float = 0.3,
    min_avg_annual_return: float = 0.0,
    max_allowed_drawdown: float = -0.12,
) -> Dict:
    """
    研究层推荐器：只给建议，不自动改执行层。
    """
    if stability_df.empty or wf_table.empty:
        return {
            "decision": "HOLD",
            "reason": "insufficient_data",
            "apply_requires_manual_approval": True,
            "current_params": current_params,
            "candidate_params": None,
        }

    candidate = stability_df.iloc[0].to_dict()

    pass_flag = (
        (wf_table["test_sharpe"] > 0)
        & (wf_table["test_annual_return"] > min_avg_annual_return)
        & (wf_table["test_max_drawdown"] >= max_allowed_drawdown)
    )

    pass_ratio = float(pass_flag.mean()) if len(pass_flag) > 0 else 0.0
    avg_sharpe = float(wf_table["test_sharpe"].mean()) if "test_sharpe" in wf_table else 0.0
    avg_annual_return = float(wf_table["test_annual_return"].mean()) if "test_annual_return" in wf_table else 0.0
    worst_drawdown = float(wf_table["test_max_drawdown"].min()) if "test_max_drawdown" in wf_table else 0.0

    recommend_ok = (
        pass_ratio >= min_pass_ratio
        and avg_sharpe >= min_avg_sharpe
        and avg_annual_return >= min_avg_annual_return
        and worst_drawdown >= max_allowed_drawdown
    )

    candidate_params = {
        "top_n": int(candidate.get("top_n", current_params.get("top_n", 2))),
        "min_score": float(candidate.get("min_score", current_params.get("min_score", -0.1))),
        "vol_lookback": int(candidate.get("vol_lookback", current_params.get("vol_lookback", 20))),
    }

    return {
        "decision": "PROPOSE" if recommend_ok else "HOLD",
        "reason": "pass_gate" if recommend_ok else "gate_not_passed",
        "apply_requires_manual_approval": True,
        "current_params": current_params,
        "candidate_params": candidate_params,
        "gates": {
            "pass_ratio": pass_ratio,
            "avg_sharpe": avg_sharpe,
            "avg_annual_return": avg_annual_return,
            "worst_drawdown": worst_drawdown,
            "thresholds": {
                "min_pass_ratio": min_pass_ratio,
                "min_avg_sharpe": min_avg_sharpe,
                "min_avg_annual_return": min_avg_annual_return,
                "max_allowed_drawdown": max_allowed_drawdown,
            },
        },
    }


def _render_recommendation_markdown(rec: Dict) -> str:
    lines = [
        "# 研究层参数建议（仅建议，不自动生效）\n",
        f"- 决策: **{rec.get('decision', 'HOLD')}**",
        f"- 原因: {rec.get('reason', '')}",
        f"- 需人工审批: {rec.get('apply_requires_manual_approval', True)}\n",
        "## 当前执行参数",
        f"- {rec.get('current_params', {})}\n",
        "## 候选参数",
        f"- {rec.get('candidate_params', None)}\n",
        "## Gate检查",
        f"- {rec.get('gates', {})}\n",
        "## 审批说明",
        "- 若你确认升级，请手工创建/更新 `reports/paper_rotation_approved_params.json`",
        "- 该文件存在时，执行层将使用其中参数；否则继续沿用默认参数。",
    ]
    return "\n".join(lines) + "\n"


def save_research_recommendation(
    recommendation: Dict,
    prefix: str = "paper_rotation",
) -> Tuple[Path, Path]:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    json_path = REPORT_DIR / f"{prefix}_research_recommendation.json"
    md_path = REPORT_DIR / f"{prefix}_research_recommendation.md"

    json_path.write_text(json.dumps(recommendation, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(_render_recommendation_markdown(recommendation), encoding="utf-8")
    return json_path, md_path
