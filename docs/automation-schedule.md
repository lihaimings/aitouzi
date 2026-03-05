# 自动化运行说明（Windows）

你当前项目建议每天自动运行：
- `python scripts/run_daily_pipeline.py`

该脚本会：
1. 刷新200只ETF候选池（按流动性）
2. 更新ETF数据缓存（分批冷启动）
3. 执行Preflight检查（严格模式；FAIL则停止主流程）
4. 执行T+1一致性校验
5. 运行纸盘/研究流程并生成报告
6. 输出系统健康报告 + 风控阈值检查 + AI研究审阅报告

通知策略（减少打扰）：
- 日常默认不推送完整日报
- 仅在异常时推送（如风控失败、停盘触发、连续N天RED）
- 周度脚本推送“周收益+风控状态+建议决策”纯文字摘要

抓数增强说明（免费源）：
- 默认按 `akshare -> efinance -> tushare -> baostock` 依次回退
- 单个代码失败时会自动重试并保留旧缓存（标记为 stale，不中断全流程）
- 对无本地缓存的新标的采用“分批冷启动”（默认每次10个），避免一次性全量拉取阻塞
- 会输出抓数状态报告：
  - `reports/paper_rotation_fetch_status.json`
  - `reports/paper_rotation_fetch_status.csv`
- 会输出模拟前检查报告：
  - `reports/paper_rotation_preflight.json`
  - `reports/paper_rotation_preflight.md`
- 会输出T+1一致性与系统健康报告：
  - `reports/paper_rotation_tplus1_check.json`
  - `reports/system_health.json`

## 方式一：任务计划程序（推荐）

1. 打开“任务计划程序”
2. 创建基本任务（例如：`AIQuantDaily`）
3. 触发器：每天，建议收盘后（如 16:30）
4. 操作：启动程序
   - 程序/脚本：你的 Python 可执行文件路径
   - 添加参数：`scripts/run_daily_pipeline.py`
   - 起始于：项目根目录（例如 `D:\project\aitouzi`）
5. 勾选失败重试（可选）

## 方式二：手工运行

```powershell Terminal
python scripts/run_daily_pipeline.py
```

## 月度复盘（建议每月一次）

```powershell Terminal
python scripts/run_monthly_review.py
```

输出：
- `reports/paper_rotation_monthly_review.md`

## 周度研究复盘（建议每周一次）

```powershell Terminal
python scripts/run_weekly_research.py
```

该脚本会：
1. 跑一遍日常流水线（更新数据+主流程）
2. 生成月度复盘与AI审阅报告
3. 生成周度研究复盘并推送飞书

输出：
- `reports/paper_rotation_weekly_review.md`

## 单独运行 AI 研究审阅（可选）

```powershell Terminal
python scripts/run_ai_research_review.py
```

输出：
- `reports/paper_rotation_ai_review.json`
- `reports/paper_rotation_ai_review.md`

可通过环境变量关闭主流程中的AI审阅：
- `ENABLE_AI_RESEARCH_REVIEW=0`

## 参数审批模板（半自动）

1. 根据研究建议生成审批模板：

```powershell Terminal
python scripts/build_approval_template.py
```

输出：
- `reports/paper_rotation_approved_params_template.json`

2. 人工复核模板参数，确认后应用为生效参数：

```powershell Terminal
python scripts/apply_approved_template.py
```

会生成/更新：
- `reports/paper_rotation_approved_params.json`
- （若原文件存在）`reports/paper_rotation_approved_params.backup.json`

## 注意事项

- 若当天数据源异常，脚本会打印 warning/error；次日重试即可
- 研究建议默认不会自动改策略参数
- `scripts/fetch_etf_cache.py` 会优先更新主ETF池，并自动补齐关键基准（510300/510500/159915）
- 若你决定升级参数，可按“参数审批模板（半自动）”流程操作
