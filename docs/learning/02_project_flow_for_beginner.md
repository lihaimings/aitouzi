# 你当前项目的实际流程（小白版）

这份文档是把你项目的真实执行链路翻译成“人话”。

## 一、每天运行时，系统到底做了什么

你执行：

```bash
python scripts/run_daily_pipeline.py
```

系统会按顺序做三件事：

1) 刷新ETF池（目标200只）
- 脚本：`scripts/refresh_etf_universe.py`
- 产物：`data/etf_universe.csv`
- 逻辑：优先挑流动性更好的ETF，包含关键基准。

2) 更新ETF数据缓存（免费数据源）
- 脚本：`scripts/fetch_etf_cache.py`
- 数据源回退链路：`akshare -> efinance -> tushare -> baostock`
- 机制：重试、回退、分批冷启动、失败保留旧缓存。
- 产物：
  - `reports/paper_rotation_fetch_status.csv`
  - `reports/paper_rotation_fetch_status.json`

3) 跑策略研究与纸交易
- 脚本：`scripts/run_paper_rotation.py`
- 关键输出：
  - 每日报告、回测指标、持仓权重、成交记录
  - 风控阈值检查
  - AI研究建议（仅建议，不自动执行）
  - 飞书通知

## 二、你项目里的“安全护栏”

你这个项目已经是健康架构，不是“AI乱下单”：

- AI只做研究建议，不直接改交易逻辑。
- 参数变更有审批模板，不会自动生效。
- 风控阈值独立检查：单日回撤/总回撤/仓位上限。
- 数据抓取失败时不中断主流程，防止任务全挂。

## 三、你每天应该看哪几个文件

先看这4个，够用了：

1. `reports/paper_rotation_fetch_status.csv`
   - 看今天数据是否更新成功，还是 stale/failed。

2. `reports/paper_rotation_daily.md`
   - 看今日策略摘要和执行结果。

3. `reports/paper_rotation_risk_guardrails.md`
   - 看风控有没有触发 FAIL。

4. `reports/paper_rotation_research_recommendation.md`
   - 看是否建议改参数（通常仍需 HOLD/人工审批）。

## 四、如果你是小白，先别急着做这些

- 先不要追高频交易。
- 先不要一次改很多参数。
- 先不要因为一周收益高就切到实盘。

## 五、正确升级路径（你可以照做）

1) 连续纸交易至少3个月，记录每周复盘。
2) 只在审批流程里改参数，每次改1-2个。
3) 如果连续多个市场阶段都稳定，再考虑小资金实盘。

## 六、你现在最重要的一个原则

**你要训练的是“稳定的流程能力”，不是“预测市场的天赋”。**

当流程稳定，收益会慢慢来；当流程混乱，再好的收益都不可持续。
