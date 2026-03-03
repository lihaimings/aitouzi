import time
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


def _fmt_pct(x: float) -> str:
    return f"{x * 100:.2f}%"


def build_snapshot() -> str:
    eq_path = REPORTS / "paper_rotation_equity.csv"
    wt_path = REPORTS / "paper_rotation_weights.csv"
    fills_path = REPORTS / "paper_rotation_fills.csv"

    eq = _safe_read_csv(eq_path)
    wt = _safe_read_csv(wt_path)
    fills = _safe_read_csv(fills_path)

    if eq.empty or "equity" not in eq.columns:
        return "[monitor] 暂无净值数据，请先运行 daily pipeline。"

    eq["date"] = pd.to_datetime(eq["date"], errors="coerce")
    eq = eq.dropna(subset=["date"]).sort_values("date")
    if len(eq) < 1:
        return "[monitor] 净值数据为空。"

    last = eq.iloc[-1]
    first = eq.iloc[0]
    total_ret = float(last["equity"] / first["equity"] - 1.0)

    day_ret = 0.0
    if len(eq) >= 2 and float(eq.iloc[-2]["equity"]) != 0:
        day_ret = float(last["equity"] / eq.iloc[-2]["equity"] - 1.0)

    mtd_ret = total_ret
    month_mask = eq["date"].dt.to_period("M") == eq["date"].iloc[-1].to_period("M")
    month_eq = eq.loc[month_mask]
    if len(month_eq) >= 2 and float(month_eq.iloc[0]["equity"]) != 0:
        mtd_ret = float(month_eq.iloc[-1]["equity"] / month_eq.iloc[0]["equity"] - 1.0)

    pos_text = "当前空仓"
    if not wt.empty and "date" in wt.columns:
        wt["date"] = pd.to_datetime(wt["date"], errors="coerce")
        wt = wt.dropna(subset=["date"]).sort_values("date")
        if len(wt) > 0:
            row = wt.iloc[-1].drop(labels=["date"], errors="ignore")
            row = pd.to_numeric(row, errors="coerce").dropna()
            active = row[row > 0].sort_values(ascending=False)
            if len(active) > 0:
                top = active.head(5)
                pos_text = ", ".join([f"{k}:{v:.1%}" for k, v in top.items()])

    trade_text = "0"
    if not fills.empty and "date" in fills.columns:
        fills["date"] = pd.to_datetime(fills["date"], errors="coerce")
        fills = fills.dropna(subset=["date"])
        today_trades = int((fills["date"].dt.date == eq["date"].iloc[-1].date()).sum())
        trade_text = str(today_trades)

    lines = [
        "=" * 70,
        f"[paper monitor] 最新日期: {eq['date'].iloc[-1].date()}",
        f"- 当前净值: {float(last['equity']):.6f}",
        f"- 累计收益: {_fmt_pct(total_ret)}",
        f"- 当日收益: {_fmt_pct(day_ret)}",
        f"- 当月收益: {_fmt_pct(mtd_ret)}",
        f"- 当日成交笔数: {trade_text}",
        f"- 持仓(Top): {pos_text}",
        "=" * 70,
    ]
    return "\n".join(lines)


def main(poll_seconds: int = 30):
    print(f"[monitor] 启动纸盘盈亏监控，每 {poll_seconds}s 刷新一次。")
    while True:
        print(build_snapshot())
        time.sleep(poll_seconds)


if __name__ == "__main__":
    main()
