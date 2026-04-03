# 策略主线改造收尾报告（2026-04-03）

## 收尾目标
完成将“红黄绿灯总闸门 + ETF 分型框架”从离线快照能力，接入到 `run_paper_rotation.py` 主执行链路。

## 本次完成项

### 1) 主流程接入总闸门
- 新增主流程内风险指标映射（macro/drawdown/breadth/volatility）
- 主流程读取 `config/strategy_gatekeeper.yaml`
- 产出 `paper_rotation_gatekeeper.json`
- 将闸门动作映射到执行参数：
  - `gross_exposure_mult` → 缩放 `target_vol_ann`
  - `turnover_mult` → 缩放 `max_turnover`
  - `allow_new_entries=false` 时抬高 `min_score/buy_threshold`

### 2) 主流程接入 ETF 分型
- 主流程读取 `config/etf_strategy_classes.yaml`
- 基于 `reports/etf_market_snapshot_raw.csv` 映射 ETF 名称与代码
- 产出：
  - `paper_rotation_strategy_classification.csv`
  - `paper_rotation_strategy_class_snapshot.json`
- 将 dominant class 的回测模板参数映射到执行参数：
  - `fee_bps`
  - `slippage_bps`
  - `top_n`（holding_limit 约束）

### 3) 报告侧联动
- `run_paper_rotation.py` 的 summary 新增：
  - 总闸门状态/评分/动作
  - 分型主类与模板
  - 分型统计
  - 分型快照路径与总闸门快照路径

### 4) 运行收敛能力
- 为主流程新增环境变量：`PAPER_ROTATION_MAX_CODES`
- 用于在回归/验收时限制 universe 规模，加速策略联调与收尾验证（默认不限制）。

## 关键改动文件
- `scripts/run_paper_rotation.py`
- `docs/aitouzi_etf_strategy_v1_execution_plan.md`

## 实跑验收（已执行）
```bash
cd /home/openclaw/work/aitouzi
PAPER_ROTATION_MAX_CODES=20 ENABLE_AI_RESEARCH_REVIEW=0 python scripts/run_paper_rotation.py
```

### 实跑摘要（节选）
- gatekeeper: `state=yellow`, `score=0.4365`
- gatekeeper actions: `gross_exposure_mult=0.65`, `turnover_mult=0.7`, `allow_new_entries=true`
- dominant class: `broad_index`
- class counts: `{'broad_index': 14, 'cross_border': 3, 'sector_theme': 3}`
- 回测流程完整结束（exit code 0），日报/回测/风控/分型/闸门产物均生成

## 新增/更新产物
- `reports/paper_rotation_gatekeeper.json`
- `reports/paper_rotation_strategy_class_snapshot.json`
- `reports/paper_rotation_strategy_classification.csv`
- `reports/paper_rotation_daily.md`（含闸门与分型信息）
- `reports/paper_rotation_*` 系列回测与研究产物（主流程标准产物）

## 当前结论
策略主线改造已完成收尾：
- 不是仅“离线快照”
- 已真正接入 `run_paper_rotation.py` 主执行链路
- 已可实跑、可落盘、可验收

## 后续增强（v1.2）
1. 接入更完整 ETF metadata，提高 `bond/cross_border/commodity` 分类准确率
2. 将 class bundle 更细粒度映射到 score 参数（分型权重）
3. 增加按分型的 walk-forward / stability 独立报告
