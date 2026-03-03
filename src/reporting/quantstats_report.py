from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd


def generate_quantstats_html(
    returns: pd.Series,
    output_filename: str = "paper_rotation_quantstats.html",
    benchmark: Optional[pd.Series] = None,
) -> Path:
    """
    用 quantstats 生成 HTML 报告。
    如果环境未安装 quantstats，会抛出 ImportError。
    """
    import quantstats as qs

    out_dir = Path(__file__).resolve().parents[2] / "reports"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / output_filename

    clean_returns = pd.Series(returns).dropna()
    if clean_returns.empty:
        raise ValueError("returns为空，无法生成quantstats报告")

    if benchmark is None:
        qs.reports.html(clean_returns, output=str(out_path), title="AI ETF Rotation Report")
    else:
        qs.reports.html(clean_returns, benchmark=benchmark, output=str(out_path), title="AI ETF Rotation Report")

    return out_path
