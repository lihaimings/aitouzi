import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.data_pipeline.universe import build_etf_universe, save_universe


def main() -> int:
    parser = argparse.ArgumentParser(description="Refresh ETF universe list")
    parser.add_argument("--size", type=int, default=200, help="Target ETF universe size")
    parser.add_argument("--min-amount", type=float, default=30_000_000.0, help="Minimum成交额过滤阈值")
    parser.add_argument("--min-listed-days", type=int, default=90)
    parser.add_argument("--min-amount-median-20d", type=float, default=30_000_000.0)
    parser.add_argument("--momentum-floor-60d", type=float, default=-0.10)
    parser.add_argument("--max-vol-20d-annual", type=float, default=0.60)
    parser.add_argument("--cooling-days", type=int, default=30)
    args = parser.parse_args()

    df = build_etf_universe(
        target_size=args.size,
        min_amount=args.min_amount,
        min_listed_days=args.min_listed_days,
        min_amount_median_20d=args.min_amount_median_20d,
        momentum_floor_60d=args.momentum_floor_60d,
        max_vol_20d_annual=args.max_vol_20d_annual,
        cooling_days=args.cooling_days,
    )
    out = save_universe(df)
    print(f"saved universe: {out}")
    print(f"universe size: {len(df)}")
    print(f"top 10 codes: {df['code'].head(10).tolist()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
