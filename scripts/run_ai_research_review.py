import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.research import build_ai_research_review, save_ai_research_review

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


def _build_context() -> dict:
    quality = _safe_read_csv(REPORTS / "paper_rotation_data_quality.csv")
    wf = _safe_read_csv(REPORTS / "paper_rotation_walk_forward.csv")
    stability = _safe_read_csv(REPORTS / "paper_rotation_param_stability.csv")
    benchmark = _safe_read_csv(REPORTS / "paper_rotation_benchmark_compare.csv")
    recommendation = _safe_read_json(REPORTS / "paper_rotation_research_recommendation.json")
    risk_guard = _safe_read_json(REPORTS / "paper_rotation_risk_guardrails.json")

    ctx = {
        "quality": {
            "severity_counts": quality["severity"].value_counts().to_dict() if "severity" in quality else {},
        },
        "walk_forward": {
            "rows": len(wf),
            "avg_test_sharpe": float(wf["test_sharpe"].mean()) if "test_sharpe" in wf else 0.0,
            "avg_test_annual_return": float(wf["test_annual_return"].mean()) if "test_annual_return" in wf else 0.0,
            "worst_test_drawdown": float(wf["test_max_drawdown"].min()) if "test_max_drawdown" in wf else 0.0,
        },
        "stability_top": stability.head(3).to_dict(orient="records") if not stability.empty else [],
        "benchmark_top": benchmark.head(3).to_dict(orient="records") if not benchmark.empty else [],
        "recommendation": recommendation,
        "risk_guard": risk_guard,
    }
    return ctx


def main():
    context = _build_context()
    review = build_ai_research_review(context=context)
    json_path, md_path = save_ai_research_review(review=review, prefix="paper_rotation")
    print(f"saved: {json_path}")
    print(f"saved: {md_path}")


if __name__ == "__main__":
    main()
