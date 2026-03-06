from __future__ import annotations

import json
from pathlib import Path
from typing import List

import pandas as pd

from src.data_pipeline.akshare_loader import ETF_LIST as DEFAULT_ETF_LIST

DATA_DIR = Path(__file__).resolve().parents[2] / "data"
REPORT_DIR = Path(__file__).resolve().parents[2] / "reports"
UNIVERSE_CSV = DATA_DIR / "etf_universe.csv"
UNIVERSE_JSON = REPORT_DIR / "paper_rotation_universe_summary.json"
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


def build_etf_universe(target_size: int = 200, min_amount: float = 10_000_000.0) -> pd.DataFrame:
    target_size = max(20, int(target_size))

    try:
        source_df = _fetch_sina_etf_table()
    except Exception:
        source_df = pd.DataFrame()

    if not source_df.empty:
        filtered = source_df[source_df["amount"] >= float(min_amount)].copy()
        if len(filtered) < target_size:
            filtered = source_df.copy()
        selected = filtered.head(target_size).copy()
    else:
        selected = pd.DataFrame({"code": list(DEFAULT_ETF_LIST), "name": "seed", "amount": 0.0})

    seed_codes = list(dict.fromkeys(BENCHMARK_CODES + list(DEFAULT_ETF_LIST)))
    for c in seed_codes:
        if c not in set(selected["code"].tolist()):
            selected = pd.concat([pd.DataFrame([{"code": c, "name": "seed", "amount": 0.0}]), selected], ignore_index=True)

    selected = selected.drop_duplicates(subset=["code"], keep="first")
    selected = selected.head(max(target_size, len(seed_codes))).reset_index(drop=True)
    selected["selected_rank"] = range(1, len(selected) + 1)

    return selected[["code", "name", "amount", "selected_rank"]]


def save_universe(df: pd.DataFrame) -> Path:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(UNIVERSE_CSV, index=False, encoding="utf-8-sig")

    summary = {
        "count": int(len(df)),
        "benchmarks_included": BENCHMARK_CODES,
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
