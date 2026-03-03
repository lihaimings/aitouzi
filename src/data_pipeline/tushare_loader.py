import os
import tushare as ts
import pandas as pd
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parents[2] / 'data'
DATA_DIR.mkdir(parents=True, exist_ok=True)

TS_TOKEN = os.getenv('TUSHARE_TOKEN')
_ENV_LOADED = False

ETF_LIST = [
    '510300', '510500', '159915', '512690', '512170', '512480', '512760'
]


def ts_code_from_etf(code: str) -> str:
    # 简单映射：159xxx 为 SZ，其余默认 SH（可扩展白名单）
    return f"{code}.SZ" if code.startswith('159') else f"{code}.SH"


def _load_local_env_once() -> None:
    global _ENV_LOADED
    if _ENV_LOADED:
        return

    env_path = Path(__file__).resolve().parents[2] / ".env"
    if not env_path.exists():
        _ENV_LOADED = True
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value

    _ENV_LOADED = True


def _get_pro():
    _load_local_env_once()
    token = os.getenv('TUSHARE_TOKEN', '').strip()
    return ts.pro_api(token) if token else None


def fetch_etf_daily(code: str) -> pd.DataFrame:
    pro = _get_pro()
    if pro is None:
        raise RuntimeError('TuShare token not configured')
    ts_code = ts_code_from_etf(code)
    df = pro.fund_daily(ts_code=ts_code)
    # fund_daily 返回字段：ts_code, trade_date, close, open, high, low, vol, amount
    df = df.rename(columns={
        'trade_date': 'date', 'vol': 'volume'
    })
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').reset_index(drop=True)
    return df[['date','open','high','low','close','volume','amount']]


def cache_etf_daily(code: str) -> Path:
    df = fetch_etf_daily(code)
    out = DATA_DIR / f"etf_{code}_tushare.csv"
    df.to_csv(out, index=False)
    return out


def ensure_universe_cache(codes=None):
    codes = codes or ETF_LIST
    paths = []
    for c in codes:
        try:
            paths.append(cache_etf_daily(c))
        except Exception as e:
            print(f"[warn] tushare fetch {c} failed: {e}")
    return paths
