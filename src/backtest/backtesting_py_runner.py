from __future__ import annotations

from pathlib import Path
from typing import Dict

import pandas as pd

DATA_DIR = Path(__file__).resolve().parents[2] / "data"


def _load_single_etf_ohlcv(code: str, source: str = "baostock") -> pd.DataFrame:
    candidates = [
        DATA_DIR / f"etf_{code}_{source}.csv",
        DATA_DIR / f"etf_{code}.csv",
    ]
    target = next((p for p in candidates if p.exists()), None)
    if target is None:
        raise FileNotFoundError(f"找不到ETF数据文件: {code}, source={source}")

    df = pd.read_csv(target)
    need_cols = ["date", "open", "high", "low", "close", "volume"]
    miss = [c for c in need_cols if c not in df.columns]
    if miss:
        raise ValueError(f"数据列缺失: {miss}")

    out = df[need_cols].copy()
    out["date"] = pd.to_datetime(out["date"])
    out = out.sort_values("date").set_index("date")
    out = out.rename(
        columns={
            "open": "Open",
            "high": "High",
            "low": "Low",
            "close": "Close",
            "volume": "Volume",
        }
    )
    return out


def run_backtestingpy_sma(
    code: str = "510300",
    source: str = "baostock",
    fast: int = 10,
    slow: int = 30,
    commission: float = 0.001,
) -> Dict[str, float]:
    """
    使用 backtesting.py 跑一个基准策略（SMA交叉）做对照，不替代主策略。
    """
    from backtesting import Backtest, Strategy

    data = _load_single_etf_ohlcv(code=code, source=source)

    class SmaCross(Strategy):
        def init(self):
            close = self.data.Close
            self.ma_fast = self.I(lambda x: pd.Series(x).rolling(fast).mean(), close)
            self.ma_slow = self.I(lambda x: pd.Series(x).rolling(slow).mean(), close)

        def next(self):
            if self.ma_fast[-1] > self.ma_slow[-1] and self.ma_fast[-2] <= self.ma_slow[-2]:
                self.buy()
            elif self.ma_fast[-1] < self.ma_slow[-1] and self.ma_fast[-2] >= self.ma_slow[-2]:
                self.sell()

    bt = Backtest(data, SmaCross, cash=100000, commission=commission, exclusive_orders=True)
    stats = bt.run()

    return {
        "Return [%]": float(stats.get("Return [%]", 0.0)),
        "Sharpe Ratio": float(stats.get("Sharpe Ratio", 0.0)),
        "Max. Drawdown [%]": float(stats.get("Max. Drawdown [%]", 0.0)),
        "# Trades": float(stats.get("# Trades", 0.0)),
    }
