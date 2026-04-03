from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import pandas as pd

try:
    import yaml  # type: ignore
except Exception:  # pragma: no cover
    yaml = None

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG = ROOT / "config" / "etf_strategy_classes.yaml"


DEFAULT_SAMPLE = [
    {"code": "510300", "name": "沪深300ETF"},
    {"code": "510500", "name": "中证500ETF"},
    {"code": "512480", "name": "半导体ETF"},
    {"code": "518880", "name": "黄金ETF"},
    {"code": "511010", "name": "国债ETF"},
    {"code": "513100", "name": "纳指ETF"},
]


def load_class_config(path: Path | None = None) -> Dict[str, Any]:
    cfg_path = path or DEFAULT_CONFIG
    if not cfg_path.exists() or yaml is None:
        return {}
    data = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))
    return data if isinstance(data, dict) else {}


def _classify_name(name: str, cfg: Dict[str, Any]) -> str:
    classes = cfg.get("classes", {}) if isinstance(cfg, dict) else {}
    fallback = cfg.get("fallback_class", "broad_index")
    text = str(name or "")
    for klass, meta in classes.items():
        labels = meta.get("labels", []) if isinstance(meta, dict) else []
        if any(label in text for label in labels):
            return str(klass)
    return str(fallback)


def classify_etf_frame(df: pd.DataFrame, config: Dict[str, Any] | None = None) -> pd.DataFrame:
    cfg = config or load_class_config()
    out = df.copy()
    name_col = "name" if "name" in out.columns else ("名称" if "名称" in out.columns else None)
    if name_col is None:
        out["strategy_class"] = cfg.get("fallback_class", "broad_index")
        return out
    out["strategy_class"] = out[name_col].astype(str).map(lambda x: _classify_name(x, cfg))
    classes = cfg.get("classes", {}) if isinstance(cfg, dict) else {}
    out["strategy_template"] = out["strategy_class"].map(lambda x: classes.get(x, {}).get("strategy_template", "momentum_core"))
    out["monitor_template"] = out["strategy_class"].map(lambda x: classes.get(x, {}).get("monitor_template", "broad_index_monitor"))
    out["backtest_template"] = out["strategy_class"].map(lambda x: classes.get(x, {}).get("backtest_template", "broad_index_backtest"))
    return out


def load_sample_universe(path: Path | None = None) -> pd.DataFrame:
    candidate = path or (ROOT / "reports" / "etf_market_snapshot_raw.csv")
    if candidate.exists():
        try:
            df = pd.read_csv(candidate)
            cols = [c for c in ["code", "name", "名称", "基金代码", "基金简称"] if c in df.columns]
            if cols:
                renamed = df.copy()
                if "基金代码" in renamed.columns and "code" not in renamed.columns:
                    renamed = renamed.rename(columns={"基金代码": "code"})
                if "基金简称" in renamed.columns and "name" not in renamed.columns:
                    renamed = renamed.rename(columns={"基金简称": "name"})
                if "名称" in renamed.columns and "name" not in renamed.columns:
                    renamed = renamed.rename(columns={"名称": "name"})
                return renamed[[c for c in ["code", "name"] if c in renamed.columns]].drop_duplicates().reset_index(drop=True)
        except Exception:
            pass
    return pd.DataFrame(DEFAULT_SAMPLE)


def summarize_classification(df: pd.DataFrame) -> Dict[str, Any]:
    if df.empty or "strategy_class" not in df.columns:
        return {"total": 0, "class_counts": {}}
    counts = df["strategy_class"].value_counts().sort_index().to_dict()
    return {
        "total": int(len(df)),
        "class_counts": {str(k): int(v) for k, v in counts.items()},
    }
