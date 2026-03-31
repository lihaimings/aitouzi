# Aitouzi 历史数据补齐需求（量化回测专用）

更新时间：2026-03-31 10:45

## 目标
- 回测数据在 **aitouzi 项目内** 独立建设。
- L1/L2 优先分钟级可回测数据。
- 宏观因子补齐 **近3年历史**。
- 不依赖/不改动 `etf_560860_research` 与 `gold_etf_swing_lab`。

## 本期落地
1. `src/data_pipeline/fetch_macro_history_3y.py`
   - FRED 免费源抓取3年宏观序列。
2. `src/data_pipeline/fetch_minute_history.py`
   - ETF 分钟数据抓取（优先在线抓取，失败则本地 minute 缓存兜底）。
3. `src/data_pipeline/history_coverage_report.py`
   - 统一输出覆盖率报告。
4. `scripts/run_quant_history_bootstrap.py`
   - 一键执行补数。

## 验收口径
- 宏观：合并表覆盖 >= 3年（按自然日范围）。
- 分钟：至少生成可回测结构化分钟K线（在线源失败可使用本地缓存兜底，但会标注 fallback）。
- 输出状态文件可直接回答：样本量、最早/最晚时间、成功率、fallback来源。
