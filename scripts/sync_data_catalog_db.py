#!/usr/bin/env python3
"""Sync layered ETF metadata/status into SQLite catalog.

This keeps CSV/Parquet as source-of-truth but provides database-style querying.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
REPORTS = ROOT / "reports"
LAYERS = DATA / "layers"
DB_PATH = DATA / "etf_catalog.db"


def _read(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def _write_df(con: sqlite3.Connection, table: str, df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0
    df.to_sql(table, con, if_exists="replace", index=False)
    return int(len(df))


def main() -> int:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(DB_PATH)
    try:
        rows = {}
        rows["l0_codes"] = _write_df(con, "l0_codes", _read(LAYERS / "l0_all_codes.csv"))
        rows["l1_codes"] = _write_df(con, "l1_codes", _read(LAYERS / "l1_watch_codes.csv"))
        rows["l2_codes"] = _write_df(con, "l2_codes", _read(LAYERS / "l2_core_codes.csv"))
        rows["l2_class_dynamic_plan"] = _write_df(con, "l2_class_dynamic_plan", _read(LAYERS / "l2_class_dynamic_plan.csv"))
        rows["fetch_status"] = _write_df(con, "fetch_status", _read(REPORTS / "paper_rotation_fetch_status.csv"))
        rows["fetch_history"] = _write_df(con, "fetch_history", _read(REPORTS / "paper_rotation_fetch_history.csv"))
        rows["etf_data_versions"] = _write_df(con, "etf_data_versions", _read(DATA / "etf_data_versions.csv"))
        rows["etf_metadata"] = _write_df(con, "etf_metadata", _read(DATA / "etf_metadata.csv"))

        # helpful view
        con.execute("DROP VIEW IF EXISTS v_layer_overview")
        con.execute(
            """
            CREATE VIEW v_layer_overview AS
            SELECT 'l0' AS layer, COUNT(*) AS n FROM l0_codes
            UNION ALL SELECT 'l1', COUNT(*) FROM l1_codes
            UNION ALL SELECT 'l2', COUNT(*) FROM l2_codes
            """
        )

        meta = {
            "updated_at": pd.Timestamp.now().isoformat(),
            "db_path": str(DB_PATH),
            "tables": rows,
        }
        (REPORTS / "etf_catalog_db_sync.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[OK] synced sqlite catalog: {DB_PATH}")
        print(f"[OK] tables: {rows}")
    finally:
        con.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
