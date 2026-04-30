import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.reporting.feishu_push import push_dm

report_path = ROOT / 'reports' / 'paper_trade_daily_2026-03-18.md'
text = report_path.read_text(encoding='utf-8')
push_dm(text)
print('sent:', report_path)
