import argparse
import json
import subprocess
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.reporting.feishu_push import push_dm

REPORTS = ROOT / "reports"


def _run_step(cmd, timeout_sec: int = 600):
    print(f"\n[step] {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=str(ROOT), capture_output=True, text=True, timeout=timeout_sec)
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr)
    if result.returncode != 0:
        raise RuntimeError(f"step failed: {' '.join(cmd)}")


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


def _build_weekly_markdown() -> Path:
    quality = _safe_read_csv(REPORTS / "paper_rotation_data_quality.csv")
    wf = _safe_read_csv(REPORTS / "paper_rotation_walk_forward.csv")
    regime = _safe_read_csv(REPORTS / "paper_rotation_regime_review.csv")
    risk_guard = _safe_read_json(REPORTS / "paper_rotation_risk_guardrails.json")
    recommendation = _safe_read_json(REPORTS / "paper_rotation_research_recommendation.json")
    ai_review = _safe_read_json(REPORTS / "paper_rotation_ai_review.json")

    lines = ["# 周度研究复盘（自动生成）\n"]

    if not quality.empty and "severity" in quality.columns:
        lines.append(f"- 数据质量统计: {quality['severity'].value_counts().to_dict()}")
    if not wf.empty:
        lines.append(f"- Walk-Forward窗口数: {len(wf)}")
        if "test_sharpe" in wf:
            lines.append(f"- Walk-Forward平均Sharpe: {float(wf['test_sharpe'].mean()):.3f}")
        if "test_max_drawdown" in wf:
            lines.append(f"- Walk-Forward最差回撤: {float(wf['test_max_drawdown'].min()):.2%}")

    lines.append(f"- 风控阈值状态: {risk_guard.get('status', 'UNKNOWN')}")
    lines.append(f"- 风控失败项: {risk_guard.get('fail_items', [])}")
    lines.append(f"- 研究建议决策: {recommendation.get('decision', 'HOLD')}")

    if not regime.empty:
        top = regime.sort_values("excess_return", ascending=False).iloc[0].to_dict()
        lines.append(f"- 阶段最佳超额: {top}")

    lines.append(f"- AI总结: {ai_review.get('overall_assessment', '无')}")

    lines.extend(
        [
            "\n## 下周执行建议",
            "- 若风控状态为 FAIL，先人工排查后再考虑调整参数。",
            "- 若研究建议为 PROPOSE，使用审批模板流程，不要直接改执行参数。",
            "- 保持至少一个月参数稳定观察，避免频繁调参。",
        ]
    )

    out = REPORTS / "paper_rotation_weekly_review.md"
    out.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return out


def main():
    parser = argparse.ArgumentParser(description="Run weekly research pipeline")
    parser.add_argument("--with-daily", action="store_true", help="Run daily pipeline first (may take longer)")
    parser.add_argument("--timeout-sec", type=int, default=600, help="Timeout per subprocess step")
    args = parser.parse_args()

    py = sys.executable

    # 1) 周度默认不跑抓数，避免网络波动导致长时间阻塞
    if args.with_daily:
        _run_step([py, "scripts/run_daily_pipeline.py"], timeout_sec=args.timeout_sec)
    else:
        _run_step([py, "scripts/run_paper_rotation.py"], timeout_sec=args.timeout_sec)

    # 2) 补充月度复盘与独立AI审阅（周度也沿用）
    _run_step([py, "scripts/run_monthly_review.py"], timeout_sec=args.timeout_sec)
    _run_step([py, "scripts/run_ai_research_review.py"], timeout_sec=args.timeout_sec)

    # 3) 生成周报并推送
    weekly_path = _build_weekly_markdown()
    text = (
        "周度研究复盘已完成\n"
        f"- 周报: {weekly_path}\n"
        f"- 风控检查: {REPORTS / 'paper_rotation_risk_guardrails.md'}\n"
        f"- AI研究: {REPORTS / 'paper_rotation_ai_review.md'}"
    )
    print(text)
    push_dm(text)


if __name__ == "__main__":
    main()
