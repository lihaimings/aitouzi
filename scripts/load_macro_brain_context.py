#!/usr/bin/env python3
import json
from datetime import datetime
from pathlib import Path

import yaml


def _summarize_from_ai_context(src_data: dict) -> dict:
    digest = src_data.get('wallstreetcn_live_digest') or {}
    news24 = ((src_data.get('news_storage') or {}).get('wallstreetcn_live_24h') or {})
    topic_counts = digest.get('topic_counts') or {}
    top_articles = news24.get('articles') or []

    current_state = {
        'event_flow': f"24h快讯数={digest.get('article_count_24h', 0)}，最近更新时间={digest.get('latest_item_time', '')}",
        'macro_scope': '第一类=华尔街见闻快讯；第二类=硬数据/官方源',
        'main_topics': '、'.join([k for k, v in sorted(topic_counts.items(), key=lambda kv: kv[1], reverse=True) if v][:4]) or '暂无显著主题',
    }
    trend_view = {
        '1_4w_bias': '以24h快讯流+第二类硬数据交叉验证为准',
        'notes': [
            f"快讯主题计数：{json.dumps(topic_counts, ensure_ascii=False)}",
            f"引用候选数：{len((src_data.get('news_storage') or {}).get('citation_candidates') or [])}",
            '该文件仅作aitouzi宏观背景输入，不直接映射交易指令。',
        ],
    }
    risks = {
        'downside': [f"快讯样本过少或过于单一：24h样本={digest.get('article_count_24h', 0)}"],
        'upside': ['第二类硬数据与快讯流出现同向验证时，宏观判断可信度提升'],
    }
    drivers = [k for k, v in topic_counts.items() if v]
    falsification_watch = [
        '若24h快讯主题快速切换，但第二类硬数据未确认，需要降低叙事权重',
        '若硬数据方向与快讯流相反，应优先信任第二类硬数据/官方源',
    ]

    return {
        'source_updated_at': src_data.get('updated_at'),
        'macro_narrative': {
            'current_state': current_state,
            'trend_view': trend_view,
            'drivers': drivers,
            'risks': risks,
            'falsification_watch': falsification_watch,
        },
        'extra_context': {
            'wallstreetcn_live_digest': {
                'article_count_24h': digest.get('article_count_24h', 0),
                'latest_item_time': digest.get('latest_item_time'),
                'topic_counts': topic_counts,
            },
            'top_live_titles': [x.get('title', '') for x in top_articles[:5]],
        }
    }


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

    if src.name == 'ai_context_pack.json':
        parsed = _summarize_from_ai_context(src_data)
        out = {
            'synced_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'source_file': str(src),
            'source_updated_at': parsed.get('source_updated_at'),
            'macro_narrative': parsed['macro_narrative'],
            'extra_context': parsed.get('extra_context', {}),
            'usage_contract': '仅作宏观背景输入，由aitouzi策略自行映射风险预算与仓位动作。'
        }
    else:
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
    ]
    for k, v in cs.items():
        lines.append(f"- {k}: {v}")

    lines.extend(['', '## 趋势观察'])
    bias = tv.get('1_4w_bias', '')
    if bias:
        lines.append(f"- {bias}")
    for n in tv.get('notes', []):
        lines.append(f"- {n}")

    lines.extend(['', '## 风险清单'])
    for r in rk.get('downside', []):
        lines.append(f"- 下行：{r}")
    for r in rk.get('upside', []):
        lines.append(f"- 修复：{r}")

    extra = out.get('extra_context', {})
    if extra:
        lines.extend(['', '## 快讯摘要'])
        digest = extra.get('wallstreetcn_live_digest', {})
        lines.append(f"- 24h快讯数：{digest.get('article_count_24h', 0)}")
        lines.append(f"- 最近快讯时间：{digest.get('latest_item_time', '')}")
        lines.append(f"- 主题计数：{json.dumps(digest.get('topic_counts', {}), ensure_ascii=False)}")
        for t in extra.get('top_live_titles', []):
            if t:
                lines.append(f"- 标题：{t}")

    lines.extend(['', '## 使用约束'])
    lines.append(f"- {out['usage_contract']}")

    out_md.write_text('\n'.join(lines) + '\n', encoding='utf-8')

    print(f'[OK] wrote {out_json}')
    print(f'[OK] wrote {out_md}')


if __name__ == '__main__':
    main()
