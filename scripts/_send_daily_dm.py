from pathlib import Path
from src.reporting.feishu_push import push_dm

report = Path('reports/paper_rotation_daily.md').read_text(encoding='utf-8')
msg = "【纸交易日报】\n" + report + "\n\n（仅纸盘，未触发实盘）"
push_dm(msg)
print('sent_attempted')
