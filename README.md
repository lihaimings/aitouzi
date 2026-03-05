# AI 投资（A 股与 ETF）

目标：基于开源量化与可接入的大模型 API，构建“研究→回测→组合→纸交易→报告”的最小可用链路，先在纸面验证 3–6 个月，再评估小额实盘。

不做：美股/加密实盘；不直接用 LLM 生成交易信号当唯一决策源。

优先市场：A 股、ETF（宽基/行业/主题）。

## 技术栈（初版）
- 数据：AkShare（免费），可选 TuShare（如有 token）
- 回测：vectorbt 或 Backtesting.py；必要时 backtrader
- 组合与风控：PyPortfolioOpt + 自定义约束（仓位/回撤/波动目标）
- LLM 用途：资讯/公告/研报摘要，情绪评分，研究脚本自动化（对接自有大模型 API）
- 纸交易：本地撮合器（A 股 T+1、涨跌停、停牌约束），记录信号与成交
- 报告：日报/周报自动生成 + 飞书通知

## 当前已实际用到的开源库（避免重复造轮子）
- `pandas` / `numpy`：时间序列计算与回测核心
- `akshare` / `baostock` / `tushare`（可选）：A股/ETF 数据拉取
- `requests`：对接你的 OpenAI 兼容大模型 API
- 说明：
  - 当前项目采用“轻量自研策略层 + 开源数据/工具库”模式，先快速纸盘验证
  - 下一步将优先接入 `Backtesting.py` 与 `quantstats` 做更标准化回测和报告

## 目录结构
- docs/ —— 研究笔记与调研报告
- data/ —— 原始与处理后数据（本地 Parquet/DuckDB）
- notebooks/ —— 可运行示例（ETF 轮动 + 情绪开关）
- reports/ —— 日/周报产出（图表、指标、说明）
- src/
  - data_pipeline/ —— 数据拉取与缓存（AkShare/TuShare）
  - signals/ —— 技术因子、基本因子、情绪信号（LLM 仅做加成）
  - backtest/ —— 回测与绩效评估（walk-forward、成本/滑点建模）
  - portfolio/ —— 组合优化与风险控制
  - paper_trade/ —— 纸交易撮合与订单记录
  - nlp/ —— 文本抓取、摘要、情绪（接入自有 LLM API）
  - reporting/ —— 报告生成与导出（图+表）

## 近期里程碑
1) 调研与选型清单（开源项目 10–20 个，对比与建议）✅
2) 最小可用示例：
   - ETF 趋势/轮动（周/双周/月调仓）基线策略 ✅
   - 情绪/新闻开关（LLM）作为风险调节因子 ✅（已提供 API 封装）
   - 回测报告与纸交易记录，生成日报/周报 ✅
3) 接入飞书推送与定时任务（仅纸盘）✅

## 快速开始（当前可运行）
1) 安装依赖：
   - `python -m pip install -r requirements.txt`
2) 先拉取本地 ETF 数据（AkShare 或 Baostock）
   - 先刷新200只ETF池：`python scripts/refresh_etf_universe.py --size 200`
   - 再抓数（带分批冷启动）：`python scripts/fetch_etf_cache.py --universe-size 200 --max-retries 1 --retry-sleep 1 --fresh-tolerance-days 3 --min-bootstrap-rows 240 --bootstrap-batch-size 10`
3) 运行日常流水线（推荐）：
   - `python scripts/run_daily_pipeline.py`
   - （会自动先拉取ETF数据，再执行纸盘/研究流程）

4) 一键长期运行（Windows 双击）：
   - 双击 `start_daily_runner.bat`
   - 默认每天 `18:30` 自动执行一次 `scripts/run_daily_pipeline.py`
   - 日志文件：`logs/daily_runner.log`
   - 停止运行：双击 `stop_daily_runner.bat`
4) 也可单独运行纸盘脚本：
   - `python scripts/run_paper_rotation.py`
5) 查看输出：
   - `reports/paper_rotation_equity.csv`
   - `reports/paper_rotation_weights.csv`
   - `reports/paper_rotation_fills.csv`
   - `reports/paper_rotation_daily.md`
   - `reports/paper_rotation_walk_forward.csv`
   - `reports/paper_rotation_walk_forward_equity.csv`
   - `reports/paper_rotation_data_quality.csv`
   - `reports/paper_rotation_data_quality.md`
   - `reports/paper_rotation_param_stability.csv`
   - `reports/paper_rotation_param_stability.md`
   - `reports/paper_rotation_param_stability_obj_vol10.png`（及其它vol窗口）
   - `reports/paper_rotation_exposure_scale.csv`
   - `reports/paper_rotation_benchmark_compare.csv`
   - `reports/paper_rotation_benchmark_compare.md`
    - `reports/paper_rotation_research_recommendation.json`
    - `reports/paper_rotation_research_recommendation.md`
    - `reports/paper_rotation_risk_guardrails.json`
    - `reports/paper_rotation_risk_guardrails.md`
    - `reports/paper_rotation_ai_review.json`
    - `reports/paper_rotation_ai_review.md`
    - `reports/paper_rotation_regime_review.csv`
    - `reports/paper_rotation_regime_review.md`
    - `reports/paper_rotation_weekly_review.md`（运行周度脚本后生成）
    - `reports/paper_rotation_quantstats.html`
    - `reports/paper_rotation_monthly_review.md`（运行月度复盘脚本后生成）
    - `reports/paper_rotation_fetch_status.csv`
    - `reports/paper_rotation_fetch_status.json`
    - `reports/paper_rotation_preflight.json`
    - `reports/paper_rotation_preflight.md`
    - `reports/paper_rotation_tplus1_check.json`
    - `reports/paper_rotation_tplus1_check.md`
    - `reports/system_health.json`
    - `reports/system_health.md`

## 当前程序运行流程（run_paper_rotation）
1) 自动扫描 `data/` 下可用ETF文件（优先 `etf_*_baostock.csv`）
2) 先执行数据质量审计（缺失/重复日期/异常跳变）并产出质量报告
2.1) 执行模拟前检查（数据抓取状态/缓存覆盖/基准新鲜度），输出 PASS/WARN/FAIL
3) 读取收盘价矩阵并计算轮动打分（20/60日动量）
4) 周频调仓，选 topN，叠加仓位上限和成本模型（手续费+滑点+冲击成本）
5) 执行风险预算层（波动目标 + 回撤保护冷静期）
5.1) 执行停盘阈值保护（单日亏损/月回撤触发后冷静期降仓）
5.2) 执行市场状态过滤（基准MA与波动率触发防御仓位）
6) 输出净值、权重、绩效指标（含 Sharpe/Sortino/Calmar/Alpha）
7) 生成模拟成交记录（纸盘审计）
8) 生成 Markdown 日报并通过飞书机器人推送
8.1) 运行 T+1 一致性校验与系统健康报告

## 飞书通知配置（App 模式）
- 在项目根目录 `.env` 配置：
  - `FEISHU_APP_ID`
  - `FEISHU_APP_SECRET`
  - `FEISHU_RECEIVE_ID_TYPE=chat_id`
  - `FEISHU_RECEIVE_ID`（推荐直接填）
- 如果暂时不知道 `chat_id`，可启用自动发现：
  - `FEISHU_RECEIVE_ID_AUTO_DISCOVER=1`
  - `FEISHU_RECEIVE_NAME_KEYWORD=你的群名关键词`（可选，建议填写）
- 连通性测试：
  - `python scripts/test_feishu_push.py`
- 查询群列表并拿到 `chat_id`：
  - `python scripts/feishu_discovery.py list-chats --keyword 关键词`
9) 运行 Walk-Forward 滚动验证：训练窗口选参数，测试窗口验证并拼接净值
10) 运行参数稳定性扫描（top_n / min_score / vol_lookback）并输出热力图
11) 运行多基准对照（510300 / 510500 / 主基准）并计算IR
12) 运行 Backtesting.py 单资产SMA基准策略（对照组）
13) 研究层输出参数建议（仅建议，不自动生效）
14) 风控阈值检查（最大单日回撤/总回撤/单标的权重）
15) AI研究助手输出结构化审阅建议（仅建议，不自动执行）
16) 市场阶段复盘（牛/熊/震荡分段）
17) 生成 QuantStats HTML 报告（图形化绩效，含基准对比）
18) （可选）运行月度复盘汇总脚本，输出一页检查清单

## 你提供的大模型 API 接入
默认读取环境变量：
- `LLM_BASE_URL`（默认 `http://localhost:8317/v1`）
- `LLM_API_KEY`（默认 `your-api-key-1`）
- `LLM_MODEL`（默认 `gpt-5`）

位置：`src/nlp/__init__.py`
用途：新闻/公告摘要与情绪评分（仅用于风控加成，不做唯一交易信号）

## 合规与风险
- 回测严格采用滚动与集外测试，建模交易成本（手续费/滑点/冲击成本）/T+1/涨跌停/停牌
- 不提供个性化投资建议；任何策略先纸盘验证
- 20%收益是阶段目标，不保证实现；优先控制回撤和稳定性

## 深度优化路线图
- 参见：`docs/quant-research-roadmap.md`
- 双体系手册：`docs/two-layer-architecture-and-playbook.md`
- 自动化与审批：`docs/automation-schedule.md`
- 免费数据稳定性：`docs/free-data-stack-and-stability.md`
- 当前已落地：
  - 基准对比（benchmark alpha）
  - 风险平价权重（inverse-vol）
  - 换手上限约束（max_turnover）
  - 扩展风险指标（Sortino / Calmar / WinRate）
  - 数据质量审计报告（CSV + Markdown）
  - 参数稳定性扫描与热力图（CSV + Markdown + PNG）
  - 风险预算层：波动目标（vol targeting）+ 回撤保护（kill-switch）
  - 多基准对照与信息比率（IR）报告
