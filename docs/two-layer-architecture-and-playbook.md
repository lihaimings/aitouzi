# 双体系架构与低维护运行手册（基于你的目标）

你当前的定位：
- 个人散户，投资基础有限
- 有大模型 API + IDE
- 时间有限，希望低维护
- 先模拟一年，再考虑实盘

这套项目按你的情况采用 **双体系**：

## 体系A：执行层（Execution Engine，稳定优先）

职责：
- 固定策略池（当前以 ETF 轮动为核心）
- 固定风控规则（换手上限、波动目标、回撤保护）
- 固定成本模型（手续费+滑点）
- 生成净值、成交、日报、回测报告

约束：
- 不允许 AI 直接改交易逻辑
- 不允许 AI 自动上调风险参数

## 体系B：研究层（Research Engine，AI辅助）

职责：
- 跑参数稳定性扫描
- 跑 walk-forward 验证
- 跑多基准对照（含 IR）
- 形成“候选参数建议”

约束：
- 只能输出建议，不自动生效
- 必须经过人工审批后，执行层才读取新参数

---

## 当前审批机制

执行层参数读取顺序：
1. `reports/paper_rotation_approved_params.json`（若存在）
2. 否则使用默认保守参数

研究层每次运行会输出：
- `reports/paper_rotation_research_recommendation.json`
- `reports/paper_rotation_research_recommendation.md`

当建议通过 gate（且你认可）时，再手工更新审批文件。

审批文件示例：

```json reports/paper_rotation_approved_params.json
{
  "top_n": 2,
  "min_score": -0.1,
  "vol_lookback": 20,
  "max_turnover": 0.8,
  "target_vol_ann": 0.12,
  "drawdown_stop": -0.05,
  "dd_cooldown_days": 5
}
```

---

## 低维护节奏（建议）

- 每日（自动）：运行 `python scripts/run_daily_pipeline.py`
- 每周：快速看一次日报摘要
- 每月：看 4 个核心报告（30分钟以内）
  1) 数据质量报告
  2) 参数稳定性报告
  3) 多基准对照（IR）
  4) 研究层建议报告
- 每季度：最多一次参数升级（有审批才改）

可选月度汇总：
- `python scripts/run_monthly_review.py`
- 输出：`reports/paper_rotation_monthly_review.md`

---

## 你的核心目标（更现实且可持续）

第一年目标（模拟盘）：
- 不追求20%，先验证系统稳定性
- 控制回撤，避免策略频繁切换
- 建立“研究-审批-执行”闭环

长期目标：
- 低回撤 + 可迭代 + 可复现
- 让 AI 成为研究助手，而非全自动交易员
