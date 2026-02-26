import pandas as pd
import akshare as ak
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parents[2] / 'data'
DATA_DIR.mkdir(parents=True, exist_ok=True)

ETF_LIST = [
    # 宽基与常见行业（可扩展）
    '510300',  # 沪深300
    '510500',  # 中证500
    '159915',  # 创业板
    '512690',  # 酒
    '512170',  # 医药
    '512480',  # 半导体
    '512760',  # 军工
]


def fetch_etf_daily(etf_code: str) -> pd.DataFrame:
    df = ak.fund_etf_hist_em(symbol=etf_code)
    df = df.rename(columns={
        '日期': 'date', '开盘': 'open', '收盘': 'close', '最高': 'high', '最低': 'low',
        '成交量': 'volume', '成交额': 'amount'
    })
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').reset_index(drop=True)
    return df


def cache_etf_daily(etf_code: str) -> Path:
    df = fetch_etf_daily(etf_code)
    out = DATA_DIR / f"etf_{etf_code}.parquet"
    df.to_parquet(out, index=False)
    return out


def ensure_universe_cache(codes=None):
    codes = codes or ETF_LIST
    paths = []
    for c in codes:
        try:
            paths.append(cache_etf_daily(c))
        except Exception as e:
            print(f"[warn] fetch {c} failed: {e}")
    return paths
