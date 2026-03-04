import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.backtest.vectorbt_runner import run_rotation_backtest

REPORTS = ROOT / "reports"


def main() -> int:
    idx = pd.date_range("2024-01-01", periods=6, freq="D")
    close = pd.DataFrame(
        {
            "A": [1.0, 1.0, 2.0, 2.0, 2.0, 2.0],
            "B": [1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
        },
        index=idx,
    )

    res = run_rotation_backtest(
        close_df=close,
        rebalance="D",
        top_n=1,
        fee_bps=0.0,
        slippage_bps=0.0,
        min_score=-1.0,
        impact_bps=0.0,
        regime_filter_enabled=False,
    )

    # 若实现了T+1，策略不应吃到第3天(2024-01-03)当日跳涨收益
    # 因为信号在该日收盘后形成，成交应在下一交易日生效。
    jump_day = pd.Timestamp("2024-01-03")
    jump_day_net = float(res.daily_returns.reindex([jump_day]).fillna(0.0).iloc[0])
    status = "PASS" if abs(jump_day_net) < 1e-12 else "FAIL"

    payload = {
        "status": status,
        "jump_day": str(jump_day.date()),
        "jump_day_net_return": jump_day_net,
        "note": "PASS means same-day jump not captured by newly formed signal (T+1 respected).",
    }

    REPORTS.mkdir(parents=True, exist_ok=True)
    json_path = REPORTS / "paper_rotation_tplus1_check.json"
    md_path = REPORTS / "paper_rotation_tplus1_check.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(
        "# T+1 校验\n\n"
        f"- status: **{status}**\n"
        f"- jump_day: {payload['jump_day']}\n"
        f"- jump_day_net_return: {jump_day_net:.6f}\n"
        "- 结论: PASS 说明当日新信号未吃到当日跳涨收益。\n",
        encoding="utf-8",
    )

    print(f"T+1 check: {status} | json={json_path} | md={md_path}")
    return 0 if status == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
