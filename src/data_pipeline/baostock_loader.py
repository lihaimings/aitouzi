import baostock as bs
import pandas as pd
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parents[2] / 'data'
DATA_DIR.mkdir(parents=True, exist_ok=True)

ETF_LIST = [
    '510300',  # sh
    '510500',  # sh
    '159915',  # sz
    '512690',  # sh
    '512170',  # sh
    '512480',  # sh
    '512760',  # sh
]


def code_with_exchange(code: str) -> str:
    # 简单规则：159xxx 走深市，其余示例默认上证；必要时可提供白名单覆盖
    if code.startswith('159'):
        return f"sz.{code}"
    else:
        return f"sh.{code}"


def fetch_k_daily(code: str) -> pd.DataFrame:
    lg = bs.login()
    try:
        if lg.error_code != '0':
            raise RuntimeError(f"baostock login failed: {lg.error_msg}")
        rs = bs.query_history_k_data_plus(
            code_with_exchange(code),
            fields='date,open,high,low,close,volume,amount',
            frequency='d',
            adjustflag='3'  # 不复权
        )
        if rs.error_code != '0':
            raise RuntimeError(f"baostock query failed: {rs.error_msg}")
        data_list = []
        while rs.next():
            data_list.append(rs.get_row_data())
        df = pd.DataFrame(data_list, columns=rs.fields)
        df['date'] = pd.to_datetime(df['date'])
        numeric_cols = ['open','high','low','close','volume','amount']
        for c in numeric_cols:
            df[c] = pd.to_numeric(df[c], errors='coerce')
        df = df.dropna(subset=['date']).sort_values('date').reset_index(drop=True)
        return df
    finally:
        bs.logout()


def cache_daily(code: str) -> Path:
    df = fetch_k_daily(code)
    out = DATA_DIR / f"etf_{code}_baostock.csv"
    df.to_csv(out, index=False)
    return out


def ensure_universe_cache(codes=None):
    codes = codes or ETF_LIST
    paths = []
    for c in codes:
        try:
            p = cache_daily(c)
            paths.append(p)
        except Exception as e:
            print(f"[warn] baostock fetch {c} failed: {e}")
    return paths
