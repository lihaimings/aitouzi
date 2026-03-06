import baostock as bs
import pandas as pd
import atexit
from pathlib import Path
from typing import Optional

DATA_DIR = Path(__file__).resolve().parents[2] / 'data'
DATA_DIR.mkdir(parents=True, exist_ok=True)
_LOGGED_IN = False

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


def _ensure_login() -> None:
    global _LOGGED_IN
    if _LOGGED_IN:
        return
    lg = bs.login()
    if lg.error_code != '0':
        raise RuntimeError(f"baostock login failed: {lg.error_msg}")
    _LOGGED_IN = True


def _safe_logout() -> None:
    global _LOGGED_IN
    if not _LOGGED_IN:
        return
    try:
        bs.logout()
    finally:
        _LOGGED_IN = False


atexit.register(_safe_logout)


def _query(symbol: str, start_date: Optional[str], end_date: Optional[str]):
    query_kwargs = {
        "code": symbol,
        "fields": 'date,open,high,low,close,volume,amount',
        "frequency": 'd',
        "adjustflag": '3',
    }
    if start_date:
        query_kwargs["start_date"] = start_date
    if end_date:
        query_kwargs["end_date"] = end_date
    rs = bs.query_history_k_data_plus(**query_kwargs)
    if rs.error_code != '0':
        raise RuntimeError(f"baostock query failed: {rs.error_msg}")
    data_list = []
    while rs.next():
        data_list.append(rs.get_row_data())
    return pd.DataFrame(data_list, columns=rs.fields)


def fetch_k_daily(code: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
    _ensure_login()
    code = str(code).strip()
    primary = code_with_exchange(code)
    df = _query(primary, start_date=start_date, end_date=end_date)

    if df.empty:
        alt = f"sz.{code}" if primary.startswith("sh.") else f"sh.{code}"
        df = _query(alt, start_date=start_date, end_date=end_date)

    if df.empty:
        return df

    df['date'] = pd.to_datetime(df['date'])
    numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'amount']
    for c in numeric_cols:
        df[c] = pd.to_numeric(df[c], errors='coerce')
    df = df.dropna(subset=['date', 'close']).sort_values('date').reset_index(drop=True)
    return df


def cache_daily(code: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> Path:
    df = fetch_k_daily(code, start_date=start_date, end_date=end_date)
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
