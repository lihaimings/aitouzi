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
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _zh_risk_status(status: str) -> str:
    mapping = {
        "PASS": "通过",
        "WARN": "警告",
        "FAIL": "未通过",
    }
    return mapping.get(str(status).upper(), str(status))


def _zh_decision(decision: str) -> str:
    mapping = {
        "HOLD": "保持不变",
        "PROPOSE": "建议调参（需人工审批）",
        "REJECT": "不建议执行",
    }
    return mapping.get(str(decision).upper(), str(decision))


def _latest_holdings(weights: pd.DataFrame, top_n: int = 5) -> list[tuple[str, float]]:
    if weights.empty:
        return []
    w = weights.copy()
    if "date" in w.columns:
        w = w.drop(columns=["date"])
    if w.empty:
        return []
    last = pd.to_numeric(w.iloc[-1], errors="coerce").fillna(0.0)
    last = last[last > 0].sort_values(ascending=False)
    return [(str(k), float(v)) for k, v in last.head(top_n).items()]


def _latest_trade_changes(weights: pd.DataFrame, threshold: float = 1e-4) -> tuple[list[tuple[str, float]], list[tuple[str, float]]]:
    if weights.empty:
        return [], []
    w = weights.copy()
    if "date" in w.columns:
        w = w.drop(columns=["date"])
    if len(w) < 2:
        return [], []
    cur = pd.to_numeric(w.iloc[-1], errors="coerce").fillna(0.0)
    prev = pd.to_numeric(w.iloc[-2], errors="coerce").fillna(0.0)
    delta = (cur - prev).sort_values(ascending=False)
    buys = [(str(k), float(v)) for k, v in delta.items() if v > threshold]
    sells = [(str(k), float(-v)) for k, v in delta.items() if v < -threshold]
    return buys, sells


def _build_weekly_markdown() -> Path:
    quality = _safe_read_csv(REPORTS / "paper_rotation_data_quality.csv")
    wf = _safe_read_csv(REPORTS / "paper_rotation_walk_forward.csv")
    regime = _safe_read_csv(REPORTS / "paper_rotation_regime_review.csv")
    risk_guard = _safe_read_json(REPORTS / "paper_rotation_risk_guardrails.json")
    recommendation = _safe_read_json(REPORTS / "paper_rotation_research_recommendation.json")
    ai_review = _safe_read_json(REPORTS / "paper_rotation_ai_review.json")

    equity = _safe_read_csv(REPORTS / "paper_rotation_equity.csv")
    weights = _safe_read_csv(REPORTS / "paper_rotation_weights.csv")

    lines = ["# 周度研究复盘（自动生成）\n"]

    if not quality.empty and "severity" in quality.columns:
        lines.append(f"- 数据质量统计: {quality['severity'].value_counts().to_dict()}")
    if not wf.empty:
        lines.append(f"- Walk-Forward窗口数: {len(wf)}")
        if "test_sharpe" in wf:
            lines.append(f"- Walk-Forward平均Sharpe: {float(wf['test_sharpe'].mean()):.3f}")
        if "test_max_drawdown" in wf:
            lines.append(f"- Walk-Forward最差回撤: {float(wf['test_max_drawdown'].min()):.2%}")

    lines.append(f"- 风控阈值状态: {_zh_risk_status(risk_guard.get('status', 'UNKNOWN'))}")
    lines.append(f"- 风控失败项: {risk_guard.get('fail_items', [])}")
    lines.append(f"- 研究建议决策: {_zh_decision(recommendation.get('decision', 'HOLD'))}")

    if not regime.empty:
        top = regime.sort_values("excess_return", ascending=False).iloc[0].to_dict()
        lines.append(f"- 阶段最佳超额: {top}")

    lines.append(f"- AI总结: {ai_review.get('overall_assessment', '无')}")

    weekly_return = None
    if not equity.empty and "equity" in equity.columns:
        eq = pd.to_numeric(equity["equity"], errors="coerce").dropna()
        if len(eq) >= 2:
            lookback = min(5, len(eq) - 1)
            weekly_return = float(eq.iloc[-1] / eq.iloc[-1 - lookback] - 1.0)
            lines.append(f"- 最近{lookback}个交易日收益: {weekly_return:.2%}")

    holdings = _latest_holdings(weights)
    if holdings:
        lines.append("- 当前持仓ETF: " + ", ".join([f"{c}({w:.1%})" for c, w in holdings]))

    buys, sells = _latest_trade_changes(weights)
    lines.append("- 最新调仓买入: " + (", ".join([f"{c}(+{w:.1%})" for c, w in buys[:5]]) if buys else "无"))
    lines.append("- 最新调仓卖出: " + (", ".join([f"{c}(-{w:.1%})" for c, w in sells[:5]]) if sells else "无"))

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
    risk_guard = _safe_read_json(REPORTS / "paper_rotation_risk_guardrails.json")
    recommendation = _safe_read_json(REPORTS / "paper_rotation_research_recommendation.json")
    equity = _safe_read_csv(REPORTS / "paper_rotation_equity.csv")
    weights = _safe_read_csv(REPORTS / "paper_rotation_weights.csv")

    weekly_return_text = "NA"
    if not equity.empty and "equity" in equity.columns:
        eq = pd.to_numeric(equity["equity"], errors="coerce").dropna()
        if len(eq) >= 2:
            lookback = min(5, len(eq) - 1)
            weekly_return_text = f"{float(eq.iloc[-1] / eq.iloc[-1 - lookback] - 1.0):.2%}"

    holdings = _latest_holdings(weights)
    buys, sells = _latest_trade_changes(weights)
    holdings_text = ", ".join([f"{c}({w:.1%})" for c, w in holdings]) if holdings else "无"
    buys_text = ", ".join([f"{c}(+{w:.1%})" for c, w in buys[:5]]) if buys else "无"
    sells_text = ", ".join([f"{c}(-{w:.1%})" for c, w in sells[:5]]) if sells else "无"

    text = (
        "周度收益播报\n"
        f"- 最近一周收益: {weekly_return_text}\n"
        f"- 风控状态: {_zh_risk_status(risk_guard.get('status', 'UNKNOWN'))}\n"
        f"- 风控失败项: {risk_guard.get('fail_items', [])}\n"
        f"- 研究建议: {_zh_decision(recommendation.get('decision', 'HOLD'))}\n"
        f"- 当前持仓ETF: {holdings_text}\n"
        f"- 最新调仓买入: {buys_text}\n"
        f"- 最新调仓卖出: {sells_text}"
    )
    print(text)
    push_dm(text)


if __name__ == "__main__":
    main()
