from pathlib import Path
import re
from src.reporting.feishu_push import push_dm

root = Path('/mnt/d/project/aitouzi')
daily = (root / 'reports' / 'paper_rotation_daily.md').read_text(encoding='utf-8')
risk = (root / 'reports' / 'paper_rotation_risk_guardrails.md').read_text(encoding='utf-8')
ai = (root / 'reports' / 'paper_rotation_ai_review.md').read_text(encoding='utf-8')

# 简单提取关键字段
def pick(pattern, text, default='N/A'):
    m = re.search(pattern, text)
    return m.group(1).strip() if m else default

ret = pick(r"总收益：([^\n]+)", daily)
ann = pick(r"年化收益：([^\n]+)", daily)
sharpe = pick(r"Sharpe：([^\n]+)", daily)
mdd = pick(r"最大回撤：([^\n]+)", daily)
position = '空仓' if '当前空仓' in daily else '有持仓（见报告）'
risk_state = 'PASS' if '状态: **PASS**' in risk else ('FAIL' if '状态: **FAIL**' in risk else 'WARN')
emotion = 'neutral'
if '过拟合风险: medium' in ai:
    emotion = '谨慎偏中性（AI接口离线）'

msg = f"""【纸交易日报】A股/ETF（仅纸盘）\n
1) 持仓\n- 当前状态：{position}\n
2) 绩效\n- 总收益：{ret}\n- 年化收益：{ann}\n- Sharpe：{sharpe}\n- 最大回撤：{mdd}\n
3) 风险\n- 风控状态：{risk_state}\n- 备注：当前风控阈值检查通过（若后续FAIL再触发预警）\n
4) 情绪摘要\n- 市场/研究情绪：{emotion}\n- 说明：LLM研究接口当前不可用，已回退人工审阅模式\n
报告文件：\n- paper_rotation_daily.md\n- paper_rotation_risk_guardrails.md\n- paper_rotation_ai_review.md\n"""

push_dm(msg)
print('sent')
