#!/usr/bin/env python3
import json
from datetime import datetime
from pathlib import Path

import yaml


def main():
    base = Path(__file__).resolve().parents[1]
    cfg = yaml.safe_load((base / 'config.yaml').read_text(encoding='utf-8'))
    mb = cfg.get('macro_bridge', {})
    if not mb.get('enabled', False):
        print('[SKIP] macro_bridge disabled')
        return

    src = Path(mb['source_json'])
    if not src.exists():
        raise FileNotFoundError(f'macro narrative not found: {src}')

    src_data = json.loads(src.read_text(encoding='utf-8'))

    out = {
        'synced_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'source_file': str(src),
        'source_updated_at': src_data.get('updated_at'),
        'macro_narrative': {
            'current_state': src_data.get('current_state', {}),
            'trend_view': src_data.get('trend_view', {}),
            'drivers': src_data.get('drivers', []),
            'risks': src_data.get('risks', {}),
            'falsification_watch': src_data.get('falsification_watch', []),
        },
        'usage_contract': '仅作宏观背景输入，由aitouzi策略自行映射风险预算与仓位动作。'
    }

    out_json = base / mb.get('output_json', 'reports/macro_brain_context.json')
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding='utf-8')

    out_md = base / mb.get('output_md', 'reports/macro_brain_context.md')
    cs = out['macro_narrative']['current_state']
    tv = out['macro_narrative']['trend_view']
    rk = out['macro_narrative']['risks']

    lines = [
        f"# 宏观中台同步（{out['synced_at']})",
        '',
        f"- 来源更新时间：{out['source_updated_at']}",
        f"- 来源文件：`{out['source_file']}`",
        '',
        '## 宏观现状',
        f"- 供给：{cs.get('supply', '')}",
        f"- 需求：{cs.get('demand', '')}",
        f"- 盘面：{cs.get('market', '')}",
        '',
        '## 趋势观察',
        f"- {tv.get('1_4w_bias', '')}",
    ]
    for n in tv.get('notes', []):
        lines.append(f"- {n}")

    lines.extend(['', '## 风险清单'])
    for r in rk.get('downside', []):
        lines.append(f"- 下行：{r}")
    for r in rk.get('upside', []):
        lines.append(f"- 修复：{r}")

    lines.extend(['', '## 使用约束'])
    lines.append(f"- {out['usage_contract']}")

    out_md.write_text('\n'.join(lines) + '\n', encoding='utf-8')

    print(f'[OK] wrote {out_json}')
    print(f'[OK] wrote {out_md}')


if __name__ == '__main__':
    main()
