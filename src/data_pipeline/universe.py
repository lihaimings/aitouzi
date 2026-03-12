from __future__ import annotations

import json
from datetime import date, timedelta
from math import sqrt
from pathlib import Path
from typing import Dict, List

import pandas as pd

from src.data_pipeline.akshare_loader import ETF_LIST as DEFAULT_ETF_LIST

DATA_DIR = Path(__file__).resolve().parents[2] / "data"
REPORT_DIR = Path(__file__).resolve().parents[2] / "reports"
UNIVERSE_CSV = DATA_DIR / "etf_universe.csv"
UNIVERSE_JSON = REPORT_DIR / "paper_rotation_universe_summary.json"
COOLING_CSV = DATA_DIR / "etf_cooling_pool.csv"
UNIVERSE_HISTORY_CSV = DATA_DIR / "etf_universe_history.csv"
BENCHMARK_CODES = ["510300", "510500", "159915"]
VALID_PREFIXES = (
    "159",
    "510",
    "511",
    "512",
    "513",
    "515",
    "516",
    "517",
    "518",
    "56",
    "58",
)


def _normalize_code(value: str) -> str:
    s = str(value).strip()
    if not s:
        return ""
    if "." in s:
        s = s.split(".")[-1]
    if s.startswith(("sh", "sz", "SH", "SZ")) and len(s) >= 8:
        s = s[-6:]
    if not (len(s) == 6 and s.isdigit()):
        return ""
    if not s.startswith(VALID_PREFIXES):
        return ""
    return s


def _fetch_sina_etf_table() -> pd.DataFrame:
    import akshare as ak

    df = ak.fund_etf_category_sina(symbol="ETF基金")
    if df is None or df.empty:
        return pd.DataFrame()

    cols = list(df.columns)
    # 按位置取列，避免不同终端中文编码差异
    code_col = cols[0]
    name_col = cols[1] if len(cols) > 1 else cols[0]
    amount_col = cols[-1]

    out = pd.DataFrame(
        {
            "raw_code": df[code_col].astype(str),
            "name": df[name_col].astype(str),
            "amount": pd.to_numeric(df[amount_col], errors="coerce").fillna(0.0),
        }
    )
    out["code"] = out["raw_code"].map(_normalize_code)
    out = out[out["code"] != ""].copy()
    out = out.drop_duplicates(subset=["code"], keep="first")
    return out.sort_values("amount", ascending=False).reset_index(drop=True)


def _load_local_metrics(code: str) -> Dict[str, float]:
    path = DATA_DIR / f"etf_{code}.csv"
    if not path.exists():
        return {
            "local_rows": 0.0,
            "listed_days": 0.0,
            "amount_median_20d": float("nan"),
            "ret_60d": float("nan"),
            "vol_20d_annual": float("nan"),
            "has_local": 0.0,
        }

    try:
        df = pd.read_csv(path, usecols=["date", "close", "amount"])
    except Exception:
        return {
            "local_rows": 0.0,
            "listed_days": 0.0,
            "amount_median_20d": float("nan"),
            "ret_60d": float("nan"),
            "vol_20d_annual": float("nan"),
            "has_local": 0.0,
        }

    if df.empty:
        return {
            "local_rows": 0.0,
            "listed_days": 0.0,
            "amount_median_20d": float("nan"),
            "ret_60d": float("nan"),
            "vol_20d_annual": float("nan"),
            "has_local": 0.0,
        }

    close = pd.to_numeric(df["close"], errors="coerce")
    amount = pd.to_numeric(df["amount"], errors="coerce")
    local_rows = int(len(df))
    listed_days = int(close.notna().sum())

    amount_median_20d = float(amount.tail(20).median()) if amount.notna().any() else float("nan")
    ret_60d = float("nan")
    if len(close) >= 60:
        c0 = float(close.iloc[-60])
        c1 = float(close.iloc[-1])
        if c0 > 0:
            ret_60d = c1 / c0 - 1.0

    vol_20d_annual = float("nan")
    if len(close) >= 20:
        daily_ret = close.pct_change().dropna()
        if len(daily_ret) >= 20:
            vol_20d_annual = float(daily_ret.tail(20).std(ddof=0) * sqrt(252.0))

    return {
        "local_rows": float(local_rows),
        "listed_days": float(listed_days),
        "amount_median_20d": amount_median_20d,
        "ret_60d": ret_60d,
        "vol_20d_annual": vol_20d_annual,
        "has_local": 1.0,
    }


def _load_cooling_state(today: date) -> pd.DataFrame:
    if not COOLING_CSV.exists():
        return pd.DataFrame(columns=["code", "cool_start", "cool_until", "reason", "source", "active"])

    try:
        df = pd.read_csv(COOLING_CSV, dtype={"code": str})
    except Exception:
        return pd.DataFrame(columns=["code", "cool_start", "cool_until", "reason", "source", "active"])

    if df.empty:
        return pd.DataFrame(columns=["code", "cool_start", "cool_until", "reason", "source", "active"])

    df["code"] = df["code"].map(_normalize_code)
    df = df[df["code"] != ""].copy()
    if "cool_until" not in df.columns:
        df["cool_until"] = str(today)
    df["cool_until"] = pd.to_datetime(df["cool_until"], errors="coerce").dt.date
    df["active"] = df["cool_until"].apply(lambda d: bool(pd.notna(d) and d >= today))
    return df


def _save_cooling_state(df: pd.DataFrame) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    keep_cols = ["code", "cool_start", "cool_until", "reason", "source", "active"]
    out = df.copy()
    for c in keep_cols:
        if c not in out.columns:
            out[c] = ""
    out = out[keep_cols].drop_duplicates(subset=["code"], keep="last")
    out.to_csv(COOLING_CSV, index=False, encoding="utf-8-sig")


def _append_universe_history(df: pd.DataFrame) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if df.empty:
        return
    out = df[["code", "selected_rank"]].copy()
    out["as_of"] = pd.Timestamp.now().date().isoformat()
    out = out[["as_of", "code", "selected_rank"]]

    if UNIVERSE_HISTORY_CSV.exists():
        try:
            old = pd.read_csv(UNIVERSE_HISTORY_CSV, dtype={"code": str})
            merged = pd.concat([old, out], ignore_index=True)
        except Exception:
            merged = out
    else:
        merged = out
    merged.to_csv(UNIVERSE_HISTORY_CSV, index=False, encoding="utf-8-sig")


def build_etf_universe(
    target_size: int = 200,
    min_amount: float = 30_000_000.0,
    min_listed_days: int = 90,
    min_amount_median_20d: float = 30_000_000.0,
    momentum_floor_60d: float = -0.10,
    max_vol_20d_annual: float = 0.60,
    cooling_days: int = 30,
) -> pd.DataFrame:
    target_size = max(20, int(target_size))
    today = pd.Timestamp.now().date()
    cooling_days = max(1, int(cooling_days))

    try:
        source_df = _fetch_sina_etf_table()
    except Exception:
        source_df = pd.DataFrame()

    if not source_df.empty:
        filtered = source_df[source_df["amount"] >= float(min_amount)].copy()
        if len(filtered) < target_size:
            filtered = source_df.copy()
        candidates = filtered.copy()
    else:
        candidates = pd.DataFrame({"code": list(DEFAULT_ETF_LIST), "name": "seed", "amount": 0.0})

    metrics = [_load_local_metrics(c) for c in candidates["code"].astype(str).tolist()]
    mdf = pd.DataFrame(metrics)
    candidates = pd.concat([candidates.reset_index(drop=True), mdf.reset_index(drop=True)], axis=1)

    has_local = candidates["has_local"] > 0.5
    listed_ok = candidates["listed_days"] >= float(min_listed_days)
    liquidity_ok = candidates["amount_median_20d"] >= float(min_amount_median_20d)
    momentum_ok = candidates["ret_60d"].fillna(0.0) >= float(momentum_floor_60d)
    vol_ok = candidates["vol_20d_annual"].fillna(0.0) <= float(max_vol_20d_annual)

    # 无本地历史的新ETF先保留（靠全市场流动性入池），有历史的做可交易过滤
    tradable_mask = (~has_local) | (listed_ok & liquidity_ok & momentum_ok & vol_ok)
    candidates = candidates[tradable_mask].copy()

    cooling = _load_cooling_state(today=today)
    active_cooling = set(cooling[cooling["active"] == True]["code"].astype(str).tolist())

    if UNIVERSE_CSV.exists():
        try:
            prev = pd.read_csv(UNIVERSE_CSV, dtype={"code": str})
            prev_codes = [_normalize_code(x) for x in prev.get("code", [])]
            prev_codes = [x for x in prev_codes if x]
        except Exception:
            prev_codes = []
    else:
        prev_codes = []

    prev_set = set(prev_codes)
    prev_rows = candidates[candidates["code"].isin(prev_set)].copy()
    weak_prev = prev_rows[
        (prev_rows["has_local"] > 0.5)
        & (
            (prev_rows["ret_60d"].fillna(0.0) < float(momentum_floor_60d))
            | (prev_rows["vol_20d_annual"].fillna(0.0) > float(max_vol_20d_annual))
            | (prev_rows["amount_median_20d"].fillna(0.0) < float(min_amount_median_20d))
        )
    ]

    weak_codes = [c for c in weak_prev["code"].astype(str).tolist() if c not in BENCHMARK_CODES]
    if weak_codes:
        add_rows = []
        for code in weak_codes:
            add_rows.append(
                {
                    "code": code,
                    "cool_start": str(today),
                    "cool_until": str(today + timedelta(days=cooling_days)),
                    "reason": "weak_momentum_or_liquidity_or_high_vol",
                    "source": "auto_refresh",
                    "active": True,
                }
            )
        if add_rows:
            cooling = pd.concat([cooling, pd.DataFrame(add_rows)], ignore_index=True)
            cooling = cooling.drop_duplicates(subset=["code"], keep="last")
            cooling["cool_until"] = pd.to_datetime(cooling["cool_until"], errors="coerce").dt.date
            cooling["active"] = cooling["cool_until"].apply(lambda d: bool(pd.notna(d) and d >= today))
            active_cooling = set(cooling[cooling["active"] == True]["code"].astype(str).tolist())

    # benchmark 永不进冷宫
    active_cooling = {c for c in active_cooling if c not in BENCHMARK_CODES}
    selected = candidates[~candidates["code"].isin(active_cooling)].copy()

    if "ret_60d" not in selected.columns:
        selected["ret_60d"] = 0.0
    selected = selected.sort_values(["amount", "ret_60d"], ascending=[False, False])

    benchmark_rows = pd.DataFrame([{"code": c, "name": "benchmark", "amount": 0.0} for c in BENCHMARK_CODES])
    selected = pd.concat([benchmark_rows, selected], ignore_index=True)
    selected = selected.drop_duplicates(subset=["code"], keep="first")
    if len(selected) < target_size:
        fallback = source_df if not source_df.empty else pd.DataFrame({"code": list(DEFAULT_ETF_LIST), "name": "seed", "amount": 0.0})
        selected = pd.concat([selected, fallback], ignore_index=True)
        selected = selected.drop_duplicates(subset=["code"], keep="first")

    selected = selected.head(target_size).reset_index(drop=True)
    selected["selected_rank"] = range(1, len(selected) + 1)
    selected["pool_tag"] = selected["code"].apply(lambda c: "benchmark" if c in BENCHMARK_CODES else "candidate")

    _save_cooling_state(cooling)
    _append_universe_history(selected)

    return selected[["code", "name", "amount", "selected_rank", "pool_tag"]]


def save_universe(df: pd.DataFrame) -> Path:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(UNIVERSE_CSV, index=False, encoding="utf-8-sig")

    summary = {
        "count": int(len(df)),
        "benchmarks_included": BENCHMARK_CODES,
        "cooling_pool_path": str(COOLING_CSV),
        "universe_history_path": str(UNIVERSE_HISTORY_CSV),
        "top10": df.head(10).to_dict(orient="records"),
    }
    UNIVERSE_JSON.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return UNIVERSE_CSV


def load_universe_codes(target_size: int = 200) -> List[str]:
    if UNIVERSE_CSV.exists():
        try:
            df = pd.read_csv(UNIVERSE_CSV, dtype={"code": str})
            codes = [_normalize_code(x) for x in df.get("code", [])]
            codes = [x for x in codes if x]
            if len(codes) >= 20:
                return list(dict.fromkeys(codes))[: max(20, int(target_size))]
        except Exception:
            pass

    df = build_etf_universe(target_size=target_size)
    save_universe(df)
    return df["code"].astype(str).tolist()
