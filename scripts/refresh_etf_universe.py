import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.data_pipeline.universe import build_etf_universe, save_universe


def main() -> int:
    parser = argparse.ArgumentParser(description="Refresh ETF universe list")
    parser.add_argument("--size", type=int, default=200, help="Target ETF universe size")
    parser.add_argument("--min-amount", type=float, default=10_000_000.0, help="Minimum成交额过滤阈值")
    args = parser.parse_args()

    df = build_etf_universe(target_size=args.size, min_amount=args.min_amount)
    out = save_universe(df)
    print(f"saved universe: {out}")
    print(f"universe size: {len(df)}")
    print(f"top 10 codes: {df['code'].head(10).tolist()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
