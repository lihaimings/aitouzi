import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.data_pipeline.akshare_loader import ETF_LIST as AK_ETF_LIST
from src.data_pipeline.akshare_loader import cache_etf_daily as ak_cache_etf_daily
from src.data_pipeline.akshare_loader import ensure_universe_cache as ak_ensure_universe_cache
from src.data_pipeline.baostock_loader import cache_daily as bs_cache_daily

DATA_DIR = ROOT / "data"
BENCHMARK_CODES = ["510300", "510500", "159915"]


def _exists_any(code: str) -> bool:
    candidates = [
        DATA_DIR / f"etf_{code}.csv",
        DATA_DIR / f"etf_{code}.parquet",
        DATA_DIR / f"etf_{code}_baostock.csv",
        DATA_DIR / f"etf_{code}_baostock.parquet",
    ]
    return any(p.exists() for p in candidates)


def _ensure_benchmarks(codes):
    ensured = []
    for code in codes:
        if _exists_any(code):
            ensured.append((code, "exists"))
            continue

        ok = False
        try:
            p = ak_cache_etf_daily(code)
            ensured.append((code, f"akshare:{p.name}"))
            ok = True
        except Exception as e:
            ensured.append((code, f"akshare_fail:{e}"))

        if not ok:
            try:
                p = bs_cache_daily(code)
                ensured.append((code, f"baostock:{p.name}"))
                ok = True
            except Exception as e:
                ensured.append((code, f"baostock_fail:{e}"))

        if not ok:
            ensured.append((code, "missing"))

    return ensured


if __name__ == "__main__":
    # 主池更新（AkShare）
    paths = ak_ensure_universe_cache(AK_ETF_LIST)

    # 基准补齐（缺失时自动尝试 AkShare -> Baostock）
    bench_status = _ensure_benchmarks(BENCHMARK_CODES)

    print("saved:")
    for p in paths:
        print(str(p))

    print("\nbenchmark ensure status:")
    for code, status in bench_status:
        print(f"- {code}: {status}")

    missing = [c for c, s in bench_status if s == "missing"]
    if missing:
        print(f"[warn] benchmark still missing: {missing}")
