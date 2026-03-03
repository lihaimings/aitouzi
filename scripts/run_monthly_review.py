import json
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / "reports"


def _safe_read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def main():
    equity_path = REPORTS / "paper_rotation_equity.csv"
    quality_path = REPORTS / "paper_rotation_data_quality.csv"
    stability_path = REPORTS / "paper_rotation_param_stability.csv"
    benchmark_path = REPORTS / "paper_rotation_benchmark_compare.csv"
    rec_path = REPORTS / "paper_rotation_research_recommendation.json"
    risk_path = REPORTS / "paper_rotation_risk_guardrails.json"
    regime_path = REPORTS / "paper_rotation_regime_review.csv"

    monthly_md = REPORTS / "paper_rotation_monthly_review.md"

    lines = ["# 月度复盘（自动生成）\n"]

    eq = _safe_read_csv(equity_path)
    if not eq.empty and "equity" in eq.columns:
        eq["date"] = pd.to_datetime(eq["date"], errors="coerce")
        eq = eq.dropna(subset=["date"]).sort_values("date")
        if len(eq) >= 2:
            total = float(eq["equity"].iloc[-1] / eq["equity"].iloc[0] - 1.0)
            lines.append(f"- 区间总收益：{total:.2%}")
            lines.append(f"- 起止：{eq['date'].iloc[0].date()} ~ {eq['date'].iloc[-1].date()}")

    quality = _safe_read_csv(quality_path)
    if not quality.empty and "severity" in quality.columns:
        counts = quality["severity"].value_counts().to_dict()
        lines.append(f"- 数据质量统计：{counts}")

    stab = _safe_read_csv(stability_path)
    if not stab.empty:
        best = stab.iloc[0].to_dict()
        lines.append(f"- 参数稳定性最佳组合：{best}")

    bench = _safe_read_csv(benchmark_path)
    if not bench.empty:
        best_bench = bench.iloc[0].to_dict()
        lines.append(f"- 多基准最佳IR：{best_bench}")

    if rec_path.exists():
        try:
            rec = json.loads(rec_path.read_text(encoding="utf-8"))
            lines.append(f"- 研究建议决策：{rec.get('decision')}")
            lines.append(f"- 候选参数：{rec.get('candidate_params')}")
            lines.append(f"- 需人工审批：{rec.get('apply_requires_manual_approval')}")
        except Exception:
            pass

    if risk_path.exists():
        try:
            risk = json.loads(risk_path.read_text(encoding="utf-8"))
            lines.append(f"- 风控阈值状态：{risk.get('status')}")
            lines.append(f"- 风控失败项：{risk.get('fail_items', [])}")
        except Exception:
            pass

    regime = _safe_read_csv(regime_path)
    if not regime.empty:
        best_regime = regime.sort_values("excess_return", ascending=False).iloc[0].to_dict()
        lines.append(f"- 市场阶段最佳超额：{best_regime}")

    lines.append("\n## 人工检查清单")
    lines.append("- 是否出现数据FAIL？")
    lines.append("- 是否出现回撤显著放大？")
    lines.append("- 是否建议PROPOSE且通过人工复核？")
    lines.append("- 若审批通过，是否更新 approved_params.json？")

    monthly_md.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"saved: {monthly_md}")


if __name__ == "__main__":
    main()
