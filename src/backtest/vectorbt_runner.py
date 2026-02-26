import pandas as pd
import numpy as np
from pathlib import Path

# 占位：后续接入 vectorbt 进行回测

def simple_rotation_allocation(scores: pd.DataFrame, top_n: int = 2) -> pd.DataFrame:
    # 依据横截面得分，在每个时间点持有前 top_n 等权
    weights = pd.DataFrame(index=scores.index, columns=scores.columns)
    for dt, row in scores.iterrows():
        top = row.nlargest(top_n).index
        w = pd.Series(0, index=scores.columns)
        w[top] = 1.0 / top_n
        weights.loc[dt] = w
    return weights.fillna(method='ffill').fillna(0)
