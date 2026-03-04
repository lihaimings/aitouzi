from __future__ import annotations

from pathlib import Path
from typing import Iterable, List, Tuple

import numpy as np
import pandas as pd

DATA_DIR = Path(__file__).resolve().parents[2] / "data"
REPORT_DIR = Path(__file__).resolve().parents[2] / "reports"


def _resolve_data_path(code: str, source: str = "baostock") -> Path:
    candidates = [
        DATA_DIR / f"etf_{code}_{source}.csv",
        DATA_DIR / f"etf_{code}.csv",
    ]
    target = next((p for p in candidates if p.exists()), None)
    if target is None:
        raise FileNotFoundError(f"找不到ETF数据文件: code={code}, source={source}")
    return target


def _load_df(code: str, source: str = "baostock") -> pd.DataFrame:
    p = _resolve_data_path(code=code, source=source)
    df = pd.read_csv(p)
    if "date" not in df.columns:
        raise ValueError(f"数据缺少 date 列: {p}")
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    return df


def audit_single_etf(
    code: str,
    source: str = "baostock",
    jump_threshold: float = 0.12,
    min_rows_fail: int = 240,
    min_rows_warn: int = 500,
    severe_jump_threshold: float = 0.25,
    jump_warn_count: int = 3,
    missing_ratio_warn: float = 0.01,
) -> dict:
    df = _load_df(code=code, source=source)

    key_cols = [c for c in ["open", "high", "low", "close", "volume", "amount"] if c in df.columns]
    numeric_df = df[key_cols].apply(pd.to_numeric, errors="coerce") if key_cols else pd.DataFrame(index=df.index)

    rows = int(len(df))
    start_date = df["date"].min()
    end_date = df["date"].max()

    dup_dates = int(df["date"].duplicated().sum())
    close_na = int(pd.to_numeric(df.get("close"), errors="coerce").isna().sum()) if "close" in df.columns else rows

    missing_ratio = float(numeric_df.isna().sum().sum() / max(numeric_df.size, 1)) if not numeric_df.empty else 1.0

    non_positive_close = 0
    jump_count = 0
    max_abs_jump = 0.0
    if "close" in df.columns:
        close = pd.to_numeric(df["close"], errors="coerce")
        non_positive_close = int((close <= 0).sum())
        ret = close.pct_change().replace([np.inf, -np.inf], np.nan)
        abs_ret = ret.abs()
        jump_count = int((abs_ret > jump_threshold).sum())
        max_abs_jump = float(abs_ret.max()) if abs_ret.notna().any() else 0.0

    severity = "PASS"
    redline_reason = ""

    if close_na > 0 or non_positive_close > 0 or dup_dates > 0:
        severity = "FAIL"
        redline_reason = "invalid_close_or_dup_dates"
    elif rows < int(min_rows_fail):
        severity = "FAIL"
        redline_reason = "insufficient_history_fail"
    elif max_abs_jump >= float(severe_jump_threshold):
        severity = "FAIL"
        redline_reason = "extreme_price_jump"
    elif rows < int(min_rows_warn):
        severity = "WARN"
        redline_reason = "insufficient_history_warn"
    elif jump_count > int(jump_warn_count) or missing_ratio > float(missing_ratio_warn):
        severity = "WARN"
        redline_reason = "data_quality_warning"

    return {
        "code": code,
        "source": source,
        "rows": rows,
        "start_date": start_date.date() if pd.notna(start_date) else None,
        "end_date": end_date.date() if pd.notna(end_date) else None,
        "missing_ratio": round(missing_ratio, 6),
        "dup_dates": dup_dates,
        "close_na": close_na,
        "non_positive_close": non_positive_close,
        "jump_count": jump_count,
        "max_abs_jump": round(max_abs_jump, 6),
        "severity": severity,
        "redline_reason": redline_reason,
    }


def audit_universe(
    codes: Iterable[str],
    source: str = "baostock",
    jump_threshold: float = 0.12,
    min_rows_fail: int = 240,
    min_rows_warn: int = 500,
    severe_jump_threshold: float = 0.25,
    jump_warn_count: int = 3,
    missing_ratio_warn: float = 0.01,
) -> pd.DataFrame:
    rows: List[dict] = []
    for code in codes:
        try:
            rows.append(
                audit_single_etf(
                    code=code,
                    source=source,
                    jump_threshold=jump_threshold,
                    min_rows_fail=min_rows_fail,
                    min_rows_warn=min_rows_warn,
                    severe_jump_threshold=severe_jump_threshold,
                    jump_warn_count=jump_warn_count,
                    missing_ratio_warn=missing_ratio_warn,
                )
            )
        except Exception as e:
            rows.append(
                {
                    "code": code,
                    "source": source,
                    "rows": 0,
                    "start_date": None,
                    "end_date": None,
                    "missing_ratio": 1.0,
                    "dup_dates": 0,
                    "close_na": 0,
                    "non_positive_close": 0,
                    "jump_count": 0,
                    "max_abs_jump": 0.0,
                    "severity": "FAIL",
                    "redline_reason": "load_error",
                    "error": str(e),
                }
            )

    out = pd.DataFrame(rows)
    if out.empty:
        return out
    order_cols = [
        "code",
        "source",
        "rows",
        "start_date",
        "end_date",
        "missing_ratio",
        "dup_dates",
        "close_na",
        "non_positive_close",
        "jump_count",
        "max_abs_jump",
        "severity",
        "redline_reason",
        "error",
    ]
    for c in order_cols:
        if c not in out.columns:
            out[c] = None
    return out[order_cols].sort_values(["severity", "code"]).reset_index(drop=True)


def render_quality_markdown(df: pd.DataFrame) -> str:
    if df.empty:
        return "# 数据质量报告\n\n- 无可用数据\n"

    total = len(df)
    pass_n = int((df["severity"] == "PASS").sum())
    warn_n = int((df["severity"] == "WARN").sum())
    fail_n = int((df["severity"] == "FAIL").sum())

    lines = [
        "# 数据质量报告\n",
        "## 汇总\n",
        f"- 标的数: {total}",
        f"- PASS: {pass_n}",
        f"- WARN: {warn_n}",
        f"- FAIL: {fail_n}\n",
        "## 明细\n",
        "| code | rows | start | end | miss% | dup | close_na | nonpos_close | jump_count | max_abs_jump | severity | reason |",
        "|---|---:|---|---|---:|---:|---:|---:|---:|---:|---|---|",
    ]

    for _, r in df.iterrows():
        lines.append(
            f"| {r['code']} | {int(r['rows'])} | {r['start_date']} | {r['end_date']} | {float(r['missing_ratio'])*100:.2f}% | "
            f"{int(r['dup_dates'])} | {int(r['close_na'])} | {int(r['non_positive_close'])} | {int(r['jump_count'])} | "
            f"{float(r['max_abs_jump'])*100:.2f}% | {r['severity']} | {r.get('redline_reason', '')} |"
        )

    return "\n".join(lines) + "\n"


def save_quality_reports(df: pd.DataFrame, prefix: str = "paper_rotation") -> Tuple[Path, Path]:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = REPORT_DIR / f"{prefix}_data_quality.csv"
    md_path = REPORT_DIR / f"{prefix}_data_quality.md"

    df.to_csv(csv_path, index=False)
    md_path.write_text(render_quality_markdown(df), encoding="utf-8")
    return csv_path, md_path
