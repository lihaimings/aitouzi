from __future__ import annotations

import json
from math import sqrt
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT / "data"
REPORTS_DIR = ROOT / "reports"
LAYERS_DIR = DATA_DIR / "layers"

CLASS_SNAPSHOT_CSV = REPORTS_DIR / "etf_market_classification_snapshot.csv"
UNIVERSE_CSV = DATA_DIR / "etf_universe.csv"
METADATA_CSV = DATA_DIR / "etf_metadata.csv"
PREMIUM_SNAPSHOT_CSV = REPORTS_DIR / "etf_premium_discount_snapshot.csv"


def _norm_code(v: str) -> str:
    s = str(v).strip()
    if not s:
        return ""
    if s.endswith(".0"):
        s = s[:-2]
    if s.startswith(("sh", "sz", "SH", "SZ")) and len(s) >= 8:
        s = s[-6:]
    return s.zfill(6) if s.isdigit() and len(s) <= 6 else s


def _load_csv_any(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def load_classification_snapshot() -> pd.DataFrame:
    if not CLASS_SNAPSHOT_CSV.exists():
        return pd.DataFrame(columns=["code", "name", "amount", "class"])
    df = pd.read_csv(CLASS_SNAPSHOT_CSV)
    rename = {"代码": "code", "名称": "name", "成交额": "amount", "分类": "class"}
    for k, v in rename.items():
        if k in df.columns:
            df = df.rename(columns={k: v})
    for c in ["code", "name", "amount", "class"]:
        if c not in df.columns:
            df[c] = "" if c in {"code", "name", "class"} else 0.0
    df["code"] = df["code"].map(_norm_code)
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)
    df["class"] = df["class"].fillna("其他/未识别").astype(str)
    df = df[df["code"] != ""].drop_duplicates("code", keep="first")
    return df[["code", "name", "amount", "class"]].reset_index(drop=True)


def _load_metadata_rows() -> pd.DataFrame:
    if not METADATA_CSV.exists():
        return pd.DataFrame(columns=["code", "cache_rows"])
    m = pd.read_csv(METADATA_CSV)
    if "code" not in m.columns:
        return pd.DataFrame(columns=["code", "cache_rows"])
    m["code"] = m["code"].map(_norm_code)
    if "cache_rows" not in m.columns:
        m["cache_rows"] = 0
    m["cache_rows"] = pd.to_numeric(m["cache_rows"], errors="coerce").fillna(0)
    return m[["code", "cache_rows"]].drop_duplicates("code", keep="last")


def _load_amount20_map(codes: List[str]) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for code in codes:
        csv_path = DATA_DIR / f"etf_{code}.csv"
        pq_path = DATA_DIR / f"etf_{code}.parquet"
        df = pd.DataFrame()
        try:
            if csv_path.exists():
                df = pd.read_csv(csv_path, usecols=["amount"])
            elif pq_path.exists():
                df = pd.read_parquet(pq_path, columns=["amount"])
        except Exception:
            df = pd.DataFrame()

        if df.empty or "amount" not in df.columns:
            out[code] = float("nan")
            continue

        amount = pd.to_numeric(df["amount"], errors="coerce").dropna().tail(20)
        out[code] = float(amount.mean()) if len(amount) else float("nan")
    return out


def _load_premium_map() -> Dict[str, float]:
    df = _load_csv_any(PREMIUM_SNAPSHOT_CSV)
    if df.empty:
        return {}

    # flexible schema
    col_code = "code" if "code" in df.columns else ("代码" if "代码" in df.columns else None)
    col_val = None
    for c in ["premium_median_abs_20", "premium_discount_abs_median_20", "折溢价绝对值20日中位数", "premium_discount"]:
        if c in df.columns:
            col_val = c
            break
    if col_code is None or col_val is None:
        return {}

    s = df[[col_code, col_val]].copy()
    s[col_code] = s[col_code].map(_norm_code)
    s[col_val] = pd.to_numeric(s[col_val], errors="coerce")
    s = s.dropna(subset=[col_code, col_val])
    return {str(r[col_code]): float(r[col_val]) for _, r in s.iterrows() if str(r[col_code])}


def build_layer_pools(
    l1_size: int = 200,
    min_l1_amount: float = 30_000_000.0,
    min_l2_amount: float = 100_000_000.0,
    premium_abs_median20_threshold: float = 0.008,
) -> Dict[str, pd.DataFrame]:
    snap = load_classification_snapshot()
    if snap.empty:
        return {
            "l0": pd.DataFrame(columns=["code"]),
            "l1": pd.DataFrame(columns=["code"]),
            "l2": pd.DataFrame(columns=["code", "class", "score"]),
            "class_plan": pd.DataFrame(columns=["class", "class_count", "liquidity_share", "target_n"]),
        }

    meta = _load_metadata_rows()
    work = snap.merge(meta, on="code", how="left")
    work["cache_rows"] = pd.to_numeric(work["cache_rows"], errors="coerce").fillna(0)

    # load 20d average amount from local data cache
    amount20_map = _load_amount20_map(work["code"].tolist())
    work["avg_amount_20d"] = work["code"].map(lambda x: amount20_map.get(str(x), float("nan")))

    # load premium/discount median abs 20d if available
    premium_map = _load_premium_map()
    work["premium_abs_median_20d"] = work["code"].map(lambda x: premium_map.get(str(x), float("nan")))

    # L0: full universe coverage (daily / low frequency)
    l0 = work[["code"]].drop_duplicates("code").reset_index(drop=True)

    # L1: use universe as base, then enforce liquidity threshold by avg_amount_20d when available
    if UNIVERSE_CSV.exists():
        u = pd.read_csv(UNIVERSE_CSV)
        if "code" in u.columns:
            u["code"] = u["code"].map(_norm_code)
            u = u[u["code"] != ""].drop_duplicates("code", keep="first")
            l1 = u[["code"]].copy()
            if len(l1) > int(l1_size):
                l1 = l1.head(int(l1_size))
            if len(l1) < int(l1_size):
                extra = work[~work["code"].isin(set(l1["code"]))].sort_values("amount", ascending=False)
                l1 = pd.concat([l1, extra[["code"]].head(int(l1_size) - len(l1))], ignore_index=True)
        else:
            l1 = work.sort_values("amount", ascending=False)[["code"]].head(int(l1_size)).copy()
    else:
        l1 = work.sort_values("amount", ascending=False)[["code"]].head(int(l1_size)).copy()

    l1 = l1.drop_duplicates("code").reset_index(drop=True)

    l1w = l1.merge(work[["code", "avg_amount_20d"]], on="code", how="left")
    l1_pass = l1w[(l1w["avg_amount_20d"].isna()) | (l1w["avg_amount_20d"] >= float(min_l1_amount))][["code"]]
    if len(l1_pass) < int(l1_size):
        extra_pool = work[~work["code"].isin(set(l1_pass["code"]))]
        extra_pool = extra_pool[(extra_pool["avg_amount_20d"].isna()) | (extra_pool["avg_amount_20d"] >= float(min_l1_amount))]
        extra_pool = extra_pool.sort_values("amount", ascending=False)[["code"]].head(int(l1_size) - len(l1_pass))
        l1 = pd.concat([l1_pass, extra_pool], ignore_index=True).drop_duplicates("code").reset_index(drop=True)
    else:
        l1 = l1_pass.head(int(l1_size)).reset_index(drop=True)

    # L2: representative per class with dynamic N and confirmed scoring weights
    total_amount = float(work["amount"].sum()) if len(work) else 0.0
    plans: List[Dict] = []
    l2_rows: List[pd.DataFrame] = []

    for cls, g in work.groupby("class"):
        g = g.copy().sort_values("amount", ascending=False)
        class_count = int(len(g))
        liquidity_share = float(g["amount"].sum() / total_amount) if total_amount > 0 else 0.0

        n = int(round(sqrt(max(1, class_count)) / 2.0))
        n = max(3, min(8, n))
        if liquidity_share > 0.20:
            n = min(8, n + 1)

        filtered = g.copy()
        # hard filter #1: avg amount 20d threshold (if available)
        filtered = filtered[(filtered["avg_amount_20d"].isna()) | (filtered["avg_amount_20d"] >= float(min_l2_amount))]

        # hard filter #2: premium/discount threshold (if available)
        filtered = filtered[(filtered["premium_abs_median_20d"].isna()) | (filtered["premium_abs_median_20d"] <= float(premium_abs_median20_threshold))]

        # fallback chain
        if filtered.empty:
            filtered = g[(g["avg_amount_20d"].isna()) | (g["avg_amount_20d"] >= float(min_l1_amount))].copy()
        if filtered.empty:
            filtered = g.head(max(3, n)).copy()

        # score = liquidity45 + size30 + stability25(proxy: cache_rows)
        liq = filtered["amount"].rank(method="average", pct=True)
        size = liq  # no reliable AUM yet, amount proxy
        stab = filtered["cache_rows"].rank(method="average", pct=True)
        filtered["score"] = 0.45 * liq + 0.30 * size + 0.25 * stab

        picked = filtered.sort_values(["score", "amount"], ascending=False).head(n)

        plans.append(
            {
                "class": cls,
                "class_count": class_count,
                "liquidity_share": round(liquidity_share, 6),
                "target_n": int(n),
                "picked_n": int(len(picked)),
            }
        )
        l2_rows.append(picked[["code", "class", "score", "avg_amount_20d", "premium_abs_median_20d"]])

    l2 = (
        pd.concat(l2_rows, ignore_index=True).drop_duplicates("code", keep="first")
        if l2_rows
        else pd.DataFrame(columns=["code", "class", "score", "avg_amount_20d", "premium_abs_median_20d"])
    )
    class_plan = pd.DataFrame(plans).sort_values(["target_n", "class_count"], ascending=False).reset_index(drop=True)

    return {"l0": l0, "l1": l1, "l2": l2, "class_plan": class_plan}


def save_layer_pools(pools: Dict[str, pd.DataFrame]) -> Tuple[Path, Path, Path, Path, Path]:
    LAYERS_DIR.mkdir(parents=True, exist_ok=True)
    l0_path = LAYERS_DIR / "l0_all_codes.csv"
    l1_path = LAYERS_DIR / "l1_watch_codes.csv"
    l2_path = LAYERS_DIR / "l2_core_codes.csv"
    class_plan_path = LAYERS_DIR / "l2_class_dynamic_plan.csv"
    summary_path = LAYERS_DIR / "layer_summary.json"

    pools.get("l0", pd.DataFrame(columns=["code"])).to_csv(l0_path, index=False, encoding="utf-8-sig")
    pools.get("l1", pd.DataFrame(columns=["code"])).to_csv(l1_path, index=False, encoding="utf-8-sig")
    pools.get("l2", pd.DataFrame(columns=["code", "class", "score"])).to_csv(l2_path, index=False, encoding="utf-8-sig")
    pools.get("class_plan", pd.DataFrame(columns=["class"]).copy()).to_csv(class_plan_path, index=False, encoding="utf-8-sig")

    summary = {
        "l0_count": int(len(pools.get("l0", []))),
        "l1_count": int(len(pools.get("l1", []))),
        "l2_count": int(len(pools.get("l2", []))),
        "generated_at": pd.Timestamp.now().isoformat(),
        "files": {
            "l0": str(l0_path),
            "l1": str(l1_path),
            "l2": str(l2_path),
            "class_plan": str(class_plan_path),
        },
    }
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return l0_path, l1_path, l2_path, class_plan_path, summary_path
