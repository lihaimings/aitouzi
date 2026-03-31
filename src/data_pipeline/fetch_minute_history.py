import argparse
import json
from datetime import datetime
from pathlib import Path

import pandas as pd

BASE = Path(__file__).resolve().parents[2]


def fetch_from_akshare(symbol: str, period: str = "15") -> pd.DataFrame:
    import akshare as ak

    df = ak.fund_etf_hist_min_em(symbol=symbol, period=period, adjust="")
    if df is None or df.empty:
        return pd.DataFrame()

    col_map = {
        "时间": "datetime",
        "开盘": "open",
        "收盘": "close",
        "最高": "high",
        "最低": "low",
        "成交量": "volume",
        "成交额": "amount",
    }
    for cn, en in col_map.items():
        if cn in df.columns:
            df = df.rename(columns={cn: en})
    keep = [c for c in ["datetime", "open", "high", "low", "close", "volume", "amount"] if c in df.columns]
    df = df[keep].copy()
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    return df.dropna(subset=["datetime"]).sort_values("datetime").reset_index(drop=True)


def fallback_local(symbol: str) -> pd.DataFrame:
    p = BASE / "data" / "minute" / f"etf_{symbol}_15m.csv"
    if not p.exists():
        return pd.DataFrame()
    df = pd.read_csv(p)
    if "datetime" in df.columns:
        df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    elif "time" in df.columns:
        df = df.rename(columns={"time": "datetime"})
        df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    return df.dropna(subset=["datetime"]).sort_values("datetime").reset_index(drop=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbols", default="510300,159326,560860")
    parser.add_argument("--period", default="15")
    args = parser.parse_args()

    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]

    out_dir = BASE / "data" / "layers" / "processed" / "minute_history"
    state_dir = BASE / "state"
    out_dir.mkdir(parents=True, exist_ok=True)
    state_dir.mkdir(parents=True, exist_ok=True)

    status = {"updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "period": args.period, "symbols": {}}

    for sym in symbols:
        src = "akshare"
        try:
            df = fetch_from_akshare(sym, period=args.period)
        except Exception as e:
            df = pd.DataFrame()
            err = str(e)
        else:
            err = None

        if df.empty:
            fb = fallback_local(sym)
            if not fb.empty:
                df = fb
                src = "local_cache"

        if df.empty:
            status["symbols"][sym] = {"ok": False, "error": err or "no data"}
            continue

        out = out_dir / f"etf_{sym}_{args.period}m.csv"
        df.to_csv(out, index=False, encoding="utf-8")
        status["symbols"][sym] = {
            "ok": True,
            "rows": int(len(df)),
            "min_datetime": str(df["datetime"].min()),
            "max_datetime": str(df["datetime"].max()),
            "source": src,
            "path": str(out),
        }

    ok_count = sum(1 for v in status["symbols"].values() if v.get("ok"))
    status["summary"] = {
        "ok": ok_count > 0,
        "total": len(symbols),
        "ok_count": ok_count,
        "success_rate": round(ok_count / max(len(symbols), 1), 4),
    }

    state_path = state_dir / "minute_history_status.json"
    state_path.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(status["summary"], ensure_ascii=False, indent=2))
    if not status["summary"]["ok"]:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
