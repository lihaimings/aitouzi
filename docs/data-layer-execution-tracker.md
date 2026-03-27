# 数据层改造执行追踪（接手执行台账）

> 维护规则（已按用户要求）：
> 1) 所有待办先写入本文件，避免遗漏；
> 2) 每完成一项，立即更新状态（TODO -> DOING -> DONE/BLOCKED）；
> 3) 每次阶段结束补充“结果摘要 + 下一步”。

---

## 0. 当前目标（用户确认）
- 仅做 ETF 主线
- 数据层按 L0/L1/L2 分层改造
- 当前优先：先跑通与稳定，再补分钟级口径

---

## 1. 总任务清单（Master Checklist）

### A. 接手与稳定化
- [x] A1. 接手审计（只读）并输出接手清单
- [x] A2. 分层抓取主链路可运行（run_layered_fetch）
- [x] A3. L1 全量实跑并修复待补全问题
- [x] A4. preflight + tplus1 联调通过

### B. 规则口径收敛（对齐需求文档 9.1~9.6）
- [x] B1. 成交额阈值接入层级筛选（L1/L2）
- [ ] B2. 折溢价异常阈值（20日中位|折溢价|>0.8%）完整落地
- [x] B3. 失败状态机（2/3/5/8）持久化到 layer health state
- [x] B4. 宏观特征映射最小集（5项）接入日流程

### C. 数据存储与可查询性
- [x] C1. 建立 SQLite catalog（兼容 CSV/Parquet）
- [x] C2. catalog 同步脚本纳入 daily pipeline
- [ ] C3. 增加 catalog 查询视图（层级质量/失败追踪）

### D. 分层执行进度
- [x] D1. L2 全量（44）实跑
- [ ] D2. L0 分批（先200）完成并出统计
- [ ] D3. L0 后续批次计划（滚动到全量）

### E. 下一阶段（分钟级）
- [ ] E1. L2 15m 数据抓取与存储链路设计
- [ ] E2. L2 15m 近12个月历史回补
- [ ] E3. L2 15m 回测口径联调（成本/执行一致性）

---

## 2. 进行中任务（Now Doing）

- DOING: D2. L0 分批抓取（第一批 200）
  - 进程：`run_layered_fetch --mode l0 --l0-limit 200`
  - 状态：执行中（后台）

---

## 3. 已完成记录（Completed Log）

### 2026-03-27
1) 接手审计完成（输出“只读审计接手清单”）。
2) 修复 L1 待补全（588180），恢复 preflight/tplus1 通过。
3) 新增并接入：
   - `src/data_pipeline/layers.py`
   - `scripts/build_layer_universe.py`
   - `scripts/run_layered_fetch.py`
   - `scripts/sync_data_catalog_db.py`
4) L2 全量实跑完成：44只（ok=31, short=13, stale=0, 待补全=0）。
5) SQLite catalog 已生成：`data/etf_catalog.db`。

---

## 4. 阻塞与风险（Blockers/Risks）

1) 当前 L2 仍是“日线抓取 + 高频调度”，尚未真正落到 15m 分钟历史数据。
2) 折溢价阈值依赖 `reports/etf_premium_discount_snapshot.csv`，若该快照缺失则仅执行降级逻辑（不硬过滤）。
3) 代码工作树仍存在非本轮改造遗留改动，后续需要做一次“提交面清理”。

---

## 5. 下一步（Next Actions）

1) 等 D2（L0 第一批200）结束后，写入结果统计并更新状态。
2) 补齐 B2（折溢价阈值完整落地，含快照构建脚本）。
3) 输出阶段验收报告 v1（可运行命令、结果、剩余缺口）。
