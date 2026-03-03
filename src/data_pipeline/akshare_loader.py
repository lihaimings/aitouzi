import pandas as pd
import akshare as ak
from pathlib import Path
import time
from typing import Optional

DATA_DIR = Path(__file__).resolve().parents[2] / 'data'
DATA_DIR.mkdir(parents=True, exist_ok=True)

ETF_LIST = [
    # 宽基
    '510050',  # 上证50
    '510300',  # 沪深300
    '510500',  # 中证500
    '159915',  # 创业板
    '159949',  # 创业板50
    '159928',  # 消费

    # 行业/主题（示例池，可继续扩）
    '512100',  # 中证1000
    '512880',  # 证券
    '512690',  # 酒
    '512170',  # 医药
    '512480',  # 半导体
    '512760',  # 军工
    '512660',  # 军工（另一只）
    '515790',  # 光伏
    '515030',  # 新能源车
    '516160',  # 新能源

    # 跨市场/商品（用于分散）
    '159870',  # 化工
    '513100',  # 纳指100
    '513500',  # 标普500
    '518880',  # 黄金
]


def fetch_etf_daily(etf_code: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
    kwargs = {
        "symbol": etf_code,
        "period": "daily",
        "adjust": "",
    }
    if start_date:
        kwargs["start_date"] = start_date
    if end_date:
        kwargs["end_date"] = end_date

    df = ak.fund_etf_hist_em(**kwargs)
    df = df.rename(columns={
        '日期': 'date', '开盘': 'open', '收盘': 'close', '最高': 'high', '最低': 'low',
        '成交量': 'volume', '成交额': 'amount'
    })
    for col in ["open", "high", "low", "close", "volume", "amount"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df['date'] = pd.to_datetime(df['date'])
    df = df.dropna(subset=["date", "close"]).sort_values('date').reset_index(drop=True)
    return df


def cache_etf_daily(etf_code: str, start_date: Optional[str] = None, end_date: Optional[str] = None):
    df = fetch_etf_daily(etf_code, start_date=start_date, end_date=end_date)
    # 先写 CSV（兜底），再尝试写 Parquet
    csv_out = DATA_DIR / f"etf_{etf_code}.csv"
    df.to_csv(csv_out, index=False)
    try:
        pq_out = DATA_DIR / f"etf_{etf_code}.parquet"
        df.to_parquet(pq_out, index=False)
        return pq_out
    except Exception as e:
        print(f"[warn] parquet write failed for {etf_code}: {e}; kept CSV")
        return csv_out


def ensure_universe_cache(codes=None, batch_size: int = 2, base_sleep: int = 3, retries: int = 5):
    codes = codes or ETF_LIST
    paths = []
    for i in range(0, len(codes), batch_size):
        batch = codes[i:i+batch_size]
        for c in batch:
            ok = False
            for r in range(retries):
                try:
                    path = cache_etf_daily(c)
                    paths.append(path)
                    ok = True
                    break
                except Exception as e:
                    wait = base_sleep * (2 ** r)
                    print(f"[warn] fetch {c} failed (retry {r+1}/{retries}): {e}; sleep {wait}s")
                    time.sleep(wait)
            if not ok:
                print(f"[error] fetch {c} failed after {retries} retries")
        # 批次间隔
        time.sleep(base_sleep)
    return paths
