# 开源库盘点（聚焦 A 股 / ETF / 纸交易 / 可接入 LLM）

> 目标：尽量复用成熟开源，避免重复造轮子；优先 A 股可用的数据、回测与执行链路；LLM 用于资讯/情绪与研究自动化。

## 一、数据与终端
- AKShare — https://github.com/akfamily/akshare （A 股/基金/期货等免费数据，社区活跃）
- TuShare — https://tushare.pro/ （需 token，A 股财务/行情广泛）
- Baostock — http://baostock.com/ （免费日线，适合快速起步）
- OpenBB — https://github.com/OpenBB-finance/OpenBB （研究终端，跨市场数据聚合）

## 二、回测与量化框架
- Backtesting.py — https://github.com/kernc/backtesting.py （轻量，易上手，适合原型）
- vectorbt — https://github.com/polakowo/vectorbt （向量化高性能；注意其开源许可证含 Commons Clause）
- backtrader — https://github.com/mementum/backtrader （经典事件驱动，生态大）
- Qlib（微软）— https://github.com/microsoft/qlib （端到端量化+ML，适合中长期升级）
- RQAlpha — https://github.com/ricequant/rqalpha （中文生态成熟；仅限非商业使用）
- vn.py / VeighNa — https://github.com/vnpy/vnpy （国内实盘生态完整，含 paper account）
- zipline-reloaded — https://github.com/stefan-jansen/zipline-reloaded （事件驱动经典框架维护版）

## 三、机器学习 / 强化学习 / 金融特征
- FinRL — https://github.com/AI4Finance-Foundation/FinRL （强化学习量化，适合实验）
- pandas-ta — https://github.com/twopirllc/pandas-ta （技术指标丰富）
- mlfinlab — https://github.com/hudson-and-thames/mlfinlab （方法论强，但商业许可为主，不适合零成本起步）

## 四、组合与绩效
- PyPortfolioOpt — https://github.com/robertmartin8/PyPortfolioOpt （组合优化）
- quantstats — https://github.com/ranaroussi/quantstats （绩效可视化与HTML报告，建议加入）
- empyrical — https://github.com/quantopian/empyrical （基础绩效指标）

## 五、LLM / 情绪 / 财经 NLP
- FinGPT — https://github.com/AI4Finance-Foundation/FinGPT （财经NLP基线）
- 本地 OpenAI 兼容 API（你已提供）— `http://localhost:8317/v1`

## 六、结合你项目的推荐选型（当前阶段）
### 阶段 A（现在~3个月，纸盘验证）
1. 数据：AKShare + Baostock 双源兜底
2. 回测：当前自研轻量回测器 + Backtesting.py（便于快速迭代）
3. 风控：自定义仓位上限、换手成本、回撤约束
4. 报告：quantstats（后续）+ 当前 Markdown 日报
5. LLM：只做情绪开关/研究总结，不直连下单

### 阶段 B（验证后，准备 1 万实盘）
1. 回测/仿真引擎升级：RQAlpha 或 vn.py（看你偏研究/偏执行）
2. 执行：逐步接券商或官方仿真接口（先最小资金）
3. 监控：日报 + 风险告警（回撤阈值、异常波动）

## 七、关键注意事项（非常重要）
- 目标“20%收益”应转为“风险调整后目标”：例如年化 12%~20%，最大回撤 < 15%。
- 只看收益会导致过拟合；必须同时盯住回撤、波动、交易成本和参数稳定性。
- LLM 生成文本易“看起来合理但不可交易”，必须把输出约束为结构化分数（如 -1~1）。
- 合规上不做个性化荐股，先纸盘 3~6 个月再小资金验证。

## 八、最终建议（避免造轮子）
1) 保留当前项目主干，优先补齐“可重复回测+纸盘记录+日报输出”
2) 增加 quantstats 报告模块，提升结果可解释性
3) 先跑宽基ETF轮动（低换手），再叠加LLM情绪做仓位调节
4) 若纸盘稳定，再把执行层迁移到 vn.py / RQAlpha

说明：本清单会持续更新，重点围绕“可落地、可复现、可风控”。