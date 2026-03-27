# aitouzi 现状功能打分表 + 具体改造任务清单（散户人民币≤30万版）

> 时间：2026-03-27
> 
> 评估口径：以“个人散户、小资金、A股/ETF、低维护、先模拟后实盘”为目标，不按机构级系统打分。

---

## 一、总览结论（先看这个）

- **综合评分：82 / 100（可用且已具备成熟雏形）**
- 当前状态：
  - 能稳定跑完整链路（拉数→质检→预检→策略→回测→风控→报告→通知）
  - 已有研究/执行分层与参数审批机制
- 主要短板：
  1. 脚本入口过多、存在重复（维护成本偏高）
  2. 执行一致性（signal→order→fill偏差）缺少标准化看板
  3. 组合暴露与策略失效监控还不够“产品化”

---

## 二、现状功能打分表（1~5分）

| 模块 | 现状评分 | 结论 | 证据（代码/脚本） |
|---|---:|---|---|
| 数据层（采集+缓存+回退） | 4.5 | 多源与重试机制完整，覆盖较强 | `scripts/fetch_etf_cache.py`, `src/data_pipeline/*` |
| 数据质量层 | 4.2 | 已有质量审计与报告，接近成熟 | `src/data_pipeline/quality.py`, `run_preflight_check.py` |
| 回测验证层 | 4.3 | Walk-forward、参数稳定性、多基准已具备 | `src/backtest/stability.py`, `benchmark_compare.py` |
| 组合与风控层 | 4.1 | 波动目标/回撤保护/风控检查已落地 | `src/research/risk_control.py`, `evaluate_risk_guardrails` |
| 执行层（纸交易） | 3.8 | 有撮合与成交记录，但执行偏差分析不足 | `src/paper_trade/__init__.py`, `paper_rotation_fills.csv` |
| 监控告警层 | 4.0 | preflight/system_health/异常告警具备 | `scripts/run_preflight_check.py`, `run_system_health.py` |
| 报告层（日周月） | 4.4 | Markdown+QuantStats+多报告产物较全 | `src/reporting/*`, `scripts/run_monthly_review.py` |
| 治理层（研究/执行分离） | 4.5 | 审批参数机制清晰 | `paper_rotation_approved_params.json` + recommendation流程 |
| 低维护可运维性 | 3.6 | 自动化有了，但脚本碎片化明显 | `scripts/` 下大量 send/push 同类脚本 |

**加权总分：82/100**

---

## 三、改造任务清单（按优先级）

## P0（必须，1~2周内，先做）

### 1) 脚本入口收敛（降维护成本）
- 目标：把 `scripts/` 中重复的发送/推送脚本收敛为少量统一入口。
- 问题：目前存在 `send_* / push_* / manual_* / once_*` 多个同类脚本，未来很难维护。
- 动作：
  - 保留 3 个主入口：
    1. `run_daily_pipeline.py`
    2. `run_weekly_research.py`
    3. `run_monthly_review.py`
  - 其余通知脚本整合为统一命令：`scripts/notify_report.py --mode daily|weekly|manual`
- 交付物：
  - `docs/ops-script-map.md`（旧脚本→新入口映射）
  - 清理后的脚本目录（减少重复脚本数量 ≥ 60%）

### 2) 执行一致性看板（signal/order/fill 偏差）
- 目标：回答“策略想买什么，最终买成了什么”。
- 动作：
  - 新增 `scripts/run_execution_consistency.py`
  - 输出：
    - `reports/paper_rotation_execution_consistency.csv`
    - `reports/paper_rotation_execution_consistency.md`
  - 指标：
    - 成交偏差率（目标权重 vs 实际成交）
    - 未成交原因分类（流动性/阈值/规则限制）
    - T+1 导致的延迟影响

### 3) 组合暴露看板（集中度风险）
- 目标：避免“看起来分散，实际同一风格暴露”。
- 动作：
  - 新增 `scripts/run_exposure_report.py`
  - 输出行业/主题/风格集中度（Top1、Top3、HHI）
  - 报告：`reports/paper_rotation_exposure_report.md`

---

## P1（推荐，2~4周）

### 4) 策略失效监控（自动降权/停用）
- 目标：连续劣化时自动保护，不靠主观判断。
- 动作：
  - 新增 `scripts/run_strategy_health.py`
  - 规则示例：
    - 近8周超额收益<0 且 Sharpe<阈值 → `DEGRADED`
    - 连续N次触发 → `PAUSE_CANDIDATE`
  - 接入日报摘要与系统健康报告

### 5) 配置治理升级（单一配置源）
- 目标：减少参数散落与“改了没生效”的风险。
- 动作：
  - 统一 `config.yaml` 与 `approved params` 的覆盖优先级文档化
  - 新增 `scripts/check_config_consistency.py`

### 6) 通知降噪策略
- 目标：只报异常和关键变化。
- 动作：
  - 告警状态机：同类告警最小间隔 + 状态变化触发
  - 日常正常状态不重复推送

---

## P2（可选，1~2个月）

### 7) 实盘桥接准备（不下实盘单，仅结构对齐）
- 统一对象：`signal -> order -> fill -> reconciliation`
- 引入审计字段：request_id / idempotency_key / retry_trace

### 8) 研究层能力增强
- 增加“参数漂移监控”与“特征稳定性（PSI）”
- 提升参数审批依据，不再只看单期指标

---

## 四、建议实施顺序（最省力）

1. **先做 P0-1（脚本收敛）**：立刻降低维护负担。
2. 再做 **P0-2（执行一致性）**：解决“能跑但不透明”的核心痛点。
3. 再做 **P0-3（暴露看板）**：把组合风险可视化。
4. 之后进入 P1：策略失效监控与配置治理。

---

## 五、验收标准（Done Definition）

- 每日只需一个入口命令即可完成全流程。
- 出现异常能在报告中定位到“哪一层、哪一步、哪类问题”。
- 每周能看到：
  - 策略有效性（收益/回撤/稳定性）
  - 执行一致性（偏差原因）
  - 组合暴露（集中度风险）
- 参数变更均可追溯（谁改、何时改、为何改、改后效果）。

---

## 六、下一步（我建议）

如果你同意，我下一步直接按这个清单先做 **P0-1 脚本收敛**，并先给你一个最小可用版本：
- `scripts/notify_report.py`
- `docs/ops-script-map.md`
- 保留旧脚本兼容1周后再清理。
