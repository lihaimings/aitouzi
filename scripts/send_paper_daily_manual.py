from __future__ import annotations

import json
from pathlib import Path
import pandas as pd

from src.reporting.feishu_push import push_dm

ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / "reports"


def _safe_read_json(path: Path):
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def main():
    eq_path = REPORTS / "paper_rotation_equity.csv"
    wt_path = REPORTS / "paper_rotation_weights.csv"
    fills_path = REPORTS / "paper_rotation_fills.csv"
    risk_path = REPORTS / "paper_rotation_risk_guardrails.json"
    ai_review_path = REPORTS / "paper_rotation_ai_review.json"

    eq = pd.read_csv(eq_path)
    eq["date"] = pd.to_datetime(eq["date"], errors="coerce")
    eq = eq.dropna(subset=["date"]).sort_values("date")
    latest = eq.iloc[-1]
    prev = eq.iloc[-2] if len(eq) >= 2 else latest

    latest_date = latest["date"].date()
    equity = float(latest["equity"])
    prev_equity = float(prev["equity"])
    daily_pnl = equity - prev_equity

    init_cash = 10000.0
    cum_pnl = equity - init_cash
    cum_ret = equity / init_cash - 1.0

    wt = pd.read_csv(wt_path)
    wt["date"] = pd.to_datetime(wt["date"], errors="coerce")
    wt = wt.dropna(subset=["date"]).sort_values("date")
    wlast = wt[wt["date"] == wt["date"].max()].drop(columns=["date"]).T
    wlast.columns = ["weight"]
    wlast = wlast[wlast["weight"] > 0.0001].sort_values("weight", ascending=False)
    top_hold = [f"{k}({v:.1%})" for k, v in wlast["weight"].head(5).items()]

    buy_txt = "无"
    sell_txt = "无"
    if fills_path.exists():
        fills = pd.read_csv(fills_path)
        if not fills.empty and "date" in fills.columns:
            fills["date"] = pd.to_datetime(fills["date"], errors="coerce")
            fills = fills.dropna(subset=["date"]).sort_values("date")
            day = fills[fills["date"].dt.normalize() == fills["date"].max().normalize()].copy()
            if not day.empty:
                day["weight_change"] = pd.to_numeric(day.get("weight_change"), errors="coerce").fillna(0.0)
                g = day.groupby("symbol", as_index=False)["weight_change"].sum()
                buys = g[g["weight_change"] > 0].sort_values("weight_change", ascending=False)
                sells = g[g["weight_change"] < 0].sort_values("weight_change")
                if not buys.empty:
                    buy_txt = "，".join([f"{r.symbol}(+{float(r.weight_change):.1%})" for r in buys.itertuples()][:5])
                if not sells.empty:
                    sell_txt = "，".join([f"{r.symbol}({float(r.weight_change):.1%})" for r in sells.itertuples()][:5])

    risk = _safe_read_json(risk_path)
    risk_status = risk.get("status", "N/A")
    risk_fail = risk.get("fail_items", [])

    ai = _safe_read_json(ai_review_path)
    sentiment = ai.get("sentiment", "neutral")
    ai_tags = ai.get("risk_tags", [])

    msg = (
        f"【纸交易日报】{latest_date}\n"
        f"- 持仓Top: {'，'.join(top_hold) if top_hold else '空仓'}\n"
        f"- 调仓买入: {buy_txt}\n"
        f"- 调仓卖出: {sell_txt}\n"
        f"- 当日盈亏: {daily_pnl:+.2f} 元\n"
        f"- 累计盈亏: {cum_pnl:+.2f} 元（{cum_ret:+.2%}）\n"
        f"- 风险状态: {risk_status}"
        f"{'（' + '、'.join(risk_fail) + '）' if risk_fail else ''}\n"
        f"- 情绪摘要: {sentiment}"
        f"{'（' + '、'.join(ai_tags) + '）' if ai_tags else ''}\n"
        f"- 说明: 仅纸盘记录，未触发实盘交易"
    )

    push_dm(msg)
    print(msg)


if __name__ == "__main__":
    main()
