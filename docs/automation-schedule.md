# 自动化运行说明（Windows）

你当前项目建议每天自动运行：
- `python scripts/run_daily_pipeline.py`

该脚本会：
1. 更新ETF数据缓存
2. 运行纸盘/研究流程并生成报告

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

