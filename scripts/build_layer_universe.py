import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.data_pipeline.layers import build_layer_pools, save_layer_pools


def main() -> int:
    parser = argparse.ArgumentParser(description="Build layered ETF pools (L0/L1/L2)")
    parser.add_argument("--l1-size", type=int, default=200)
    parser.add_argument("--min-l1-amount", type=float, default=30_000_000.0)
    parser.add_argument("--min-l2-amount", type=float, default=100_000_000.0)
    parser.add_argument("--premium-threshold", type=float, default=0.008, help="20d abs premium/discount median threshold")
    args = parser.parse_args()

    pools = build_layer_pools(
        l1_size=max(20, int(args.l1_size)),
        min_l1_amount=max(0.0, float(args.min_l1_amount)),
        min_l2_amount=max(0.0, float(args.min_l2_amount)),
        premium_abs_median20_threshold=max(0.0, float(args.premium_threshold)),
    )
    l0, l1, l2, cls, summary = save_layer_pools(pools)

    print(f"[OK] L0 saved: {l0} ({len(pools['l0'])})")
    print(f"[OK] L1 saved: {l1} ({len(pools['l1'])})")
    print(f"[OK] L2 saved: {l2} ({len(pools['l2'])})")
    print(f"[OK] dynamic class plan: {cls}")
    print(f"[OK] summary: {summary}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
