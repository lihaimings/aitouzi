import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.reporting.feishu_push import push_dm


def main():
    text = """【纸交易日报｜A股/ETF｜2026-03-17】

一、持仓
- 当前持仓：空仓
- 当日交易：无新增调仓（历史成交仅早期样本：510050 买/卖各1笔）

二、绩效
- 总收益：-5.51%
- 年化收益：-0.28%
- 年化波动：0.68%
- Sharpe：-0.41
- 最大回撤：-5.81%
- 年化Alpha：-0.17%

三、风险
- 风险状态：PASS（风控红线未触发）
- 停盘触发：单日0次 / 月度0次
- 回测有效性评分：92/100（window PASS / walk-forward PASS / data_quality WARN / cost_model PASS）
- 模拟目标未达标项：annual_return、sharpe

四、情绪与研究摘要
- AI研究接口不可用（本地 LLM 服务连接失败），当前转人工审阅模式
- 市场阶段洞察：侧重震荡/偏弱阶段，策略近期超额不明显
- 情绪结论：谨慎中性（维持防守，不做激进加仓）

五、备注
- 仅纸盘模拟，不触发实盘。
- 详细文件：reports/paper_rotation_daily.md
"""
    push_dm(text)


if __name__ == "__main__":
    main()
