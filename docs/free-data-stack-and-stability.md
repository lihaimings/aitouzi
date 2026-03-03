# 免费数据源与稳定性建议

## 当前项目的免费数据栈（已接入）
- `akshare`：主源，覆盖广，接口丰富。
- `efinance`：主源故障时的免费回退源。
- `baostock`：次级回退源，适合兜底，但部分ETF覆盖可能较弱。
- `tushare`：支持基金日线，但存在积分权限门槛（并非完全免费）。

## 实操中的稳定性问题（已观测）
- AkShare / EFinance 都依赖东财链路，网络抖动时可能同时失败。
- Tushare `fund_daily` 在低积分权限下可能不可用。
- BaoStock 对部分ETF返回历史长度不足，不适合作为“冷启动主源”。

## 已落地的稳定性优化
- 新增200只ETF动态池：优先用 `ak.fund_etf_category_sina` 刷新候选列表（按成交额排序）
- 抓数脚本改为多源回退：`akshare -> efinance -> tushare -> baostock`
- 每个代码支持重试，失败时保留旧缓存并标记 `stale`
- 增加最小历史长度门槛（默认 `240` 行）
- 对“本地已新鲜且历史足够”的代码跳过慢回退源，避免任务阻塞
- 对“无本地缓存”的新标的使用分批冷启动（默认每次10个），避免200只全量首日阻塞
- 输出抓数状态报告：
  - `reports/paper_rotation_fetch_status.json`
  - `reports/paper_rotation_fetch_status.csv`
  - `reports/paper_rotation_universe_summary.json`

## 推荐运行参数（免费、稳定优先）
```powershell Terminal
python scripts/refresh_etf_universe.py --size 200
python scripts/fetch_etf_cache.py --universe-size 200 --max-retries 1 --retry-sleep 1 --fresh-tolerance-days 3 --min-bootstrap-rows 240 --bootstrap-batch-size 10
```

## 调研到的免费方案（2026）
- AKShare 文档仍活跃，ETF相关接口完整；其中 `fund_etf_category_sina` 在当前网络下可用性高于东财行情接口。
- EFinance 文档与PyPI活跃更新（0.5.5.x），可作为免费回退源。
- BaoStock PyPI最新版本 0.8.9（2024-05），免费稳定但ETF覆盖深度不如主源。
- Tushare 有明确积分门槛，`fund_daily` 需较高权限，不应作为“纯免费主链路”。

## 策略层防护（已启用）
- 回测入口只使用历史行数足够的标的（默认 `>=240` 行）
- 避免短历史数据把回测结果“拉偏”
