# 免费量化数据源调研与实测（2026-03）

## 调研目标
- 找到可用于 A 股 ETF 日线历史数据的免费开源方案
- 在本机网络环境实测可用性
- 优先能稳定跑每日自动任务的方案

## 调研到的开源方向（GitHub）
- `akfamily/akshare`：覆盖广，接口丰富，但在当前环境有连接中断波动。
- `Micro-sheep/efinance`：基于东财接口封装，实测可用。
- `shimencaiji/baostock`：免费稳定，覆盖存在差异，部分ETF历史较短。
- 东财爬虫/接口项目（多个仓库）：核心思路是直接请求 Eastmoney K 线接口。

## 本机实测结论
- 直连 Eastmoney K 线接口（`push2his.eastmoney.com`）可获取 510300/159915/511880 等ETF数据。
- `efinance` 在本机可正常获取 ETF 历史数据。
- `akshare` 在本机仍有较高概率出现 `RemoteDisconnected`。

## 已落地改造
- 新增自研免费爬虫源：`src/data_pipeline/eastmoney_loader.py`
- 抓数链路改为：`eastmoney -> efinance -> akshare -> tushare -> baostock`
- 抓数结果新增 `short` 状态（历史较短但可用），不再把这类标的误判为失败
- ETF 池代码增加前缀过滤，减少无效代码

## 当前建议
- 每日自动抓数主链路使用 eastmoney + efinance
- AkShare 保留为回退源，不作为唯一主源
- 对 `short` 标的在策略阶段继续按最小历史长度过滤
