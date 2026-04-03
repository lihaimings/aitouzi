# aitouzi ETF分型策略 v1 执行清单

## 目标
在 aitouzi 项目内落地**可运行、可落盘、可验收**的 ETF 分型策略 v1，完成：
1. 红黄绿灯总闸门
2. ETF 分型策略框架
3. 各分型最小可用监控与回测模板
4. 与现有 paper rotation / 回测 / 报告链路兼容

## v1 范围
首批覆盖 5 类 ETF：
- broad_index（宽基）
- sector_theme（行业主题）
- bond（债券）
- commodity_gold（商品/黄金）
- cross_border（跨境）

## 设计原则
- 生产优先：配置化、可审计、可扩展、可回滚
- 闸门与 alpha 分层：闸门只负责“放行/收缩/防守”
- 分类与模板分离：分类决定策略模板、监控模板、回测模板
- 新能力尽量新增文件，不打断现有日常 paper rotation 链路

## 本次落地文件
### 配置
- `config/strategy_gatekeeper.yaml`
- `config/etf_strategy_classes.yaml`

### 核心模块
- `src/strategy/gatekeeper.py`
- `src/strategy/classification.py`
- `src/strategy/templates.py`
- `src/strategy/__init__.py`

### CLI / 验收脚本
- `scripts/run_gatekeeper_snapshot.py`
- `scripts/run_classification_snapshot.py`
- `scripts/run_class_backtest.py`
- `scripts/run_class_monitor.py`

### 产物
- `reports/gatekeeper_snapshot.json|md`
- `reports/etf_strategy_classification_snapshot.csv|md`
- `reports/etf_strategy_classification_summary.json`
- `reports/etf_class_backtest_templates.json|md`
- `reports/etf_class_monitor_snapshot.json|md`
- `reports/aitouzi_etf_strategy_v1_validation.md`

## 执行清单

### M1 红黄绿灯总闸门
- [x] 总闸门配置已存在并可读取
- [x] 总闸门评分器支持：macro/drawdown/breadth/volatility 四维归一化
- [x] 输出 `green/yellow/red`
- [x] 输出动作建议：`gross_exposure_mult / turnover_mult / allow_new_entries`
- [x] 输出 JSON + Markdown 快照
- [x] 已接入 `run_paper_rotation.py` 主流程前置约束（2026-04-03 完成）

### M2 ETF 分型策略框架
- [x] 规则分类：按 ETF 名称标签映射到 5 大类
- [x] 输出 `strategy_class / strategy_template / monitor_template / backtest_template`
- [x] 支持样本宇宙 fallback（无市场快照时仍可跑）
- [x] 输出分类统计摘要
- [x] 已接入主流程分型快照与模板路由（2026-04-03 完成）
- [ ] 后续接入 ETF 元数据源进一步增强分类准确率（v1.2）

### M3 各分型最小可用监控模板
- [x] broad_index monitor
- [x] sector_theme monitor
- [x] bond monitor
- [x] commodity_gold monitor
- [x] cross_border monitor
- [x] 输出每类监控 focus/cadence 快照

### M4 各分型最小可用回测模板
- [x] broad_index backtest
- [x] sector_theme backtest
- [x] bond backtest
- [x] commodity_gold backtest
- [x] cross_border backtest
- [x] 输出每类 signal/monitor/backtest 模板 bundle

### M5 联调与验收
- [x] 闸门 CLI 可跑
- [x] 分类 CLI 可跑
- [x] 分型监控模板 CLI 可跑
- [x] 分型回测模板 CLI 可跑
- [x] 形成 `v1_validation` 报告

## 验收命令
```bash
cd /home/openclaw/work/aitouzi
python scripts/run_gatekeeper_snapshot.py
python scripts/run_classification_snapshot.py
python scripts/run_class_monitor.py
python scripts/run_class_backtest.py
```

## v1 通过标准
- 能生成总闸门 JSON/MD 快照
- 能输出 ETF 分类结果及统计
- 能导出每类最小可用监控模板
- 能导出每类最小可用回测模板
- 脚本可直接运行，不依赖手工编辑代码

## 下一步（生产化补强）
1. 接入 `run_paper_rotation.py`：总闸门前置约束组合风险暴露
2. 接入真实 ETF metadata：增强“债券 / 跨境 / 商品”分类准确率
3. 将 class bundle 接入回测主函数，实现“按类自动路由参数”
4. 为每类补专属风险指标：
   - broad_index：breadth + benchmark regime
   - sector_theme：拥挤度 + 波动扩张
   - bond：利率/信用利差
   - commodity_gold：宏观冲击 + 波动灯
   - cross_border：海外指数 + 汇率波动
5. 增加 walk-forward / param stability 的分型版本

## 回滚方案
- 新能力均为新增模块和脚本
- 现有 `run_paper_rotation.py` 默认行为不变
- 如需停用，只需不调用新增脚本/不接入主流程
