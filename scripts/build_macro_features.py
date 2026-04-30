#!/usr/bin/env python3
import json
from datetime import datetime
from pathlib import Path

import yaml


def _bounded(x: float) -> float:
    return max(0.0, min(100.0, float(x)))


def _contains_any(text: str, keys: list[str]) -> bool:
    t = (text or "").lower()
    return any(k.lower() in t for k in keys)


def _extract_text_blob(src_data: dict, src: Path) -> str:
    if src.name != "ai_context_pack.json":
        return json.dumps(src_data, ensure_ascii=False)

    digest = src_data.get("wallstreetcn_live_digest") or {}
    news24 = ((src_data.get("news_storage") or {}).get("wallstreetcn_live_24h") or {})
    articles = news24.get("articles") or []
    titles = [x.get("title", "") for x in articles[:20] if x.get("title")]
    parts = [
        json.dumps(digest.get("topic_counts") or {}, ensure_ascii=False),
        " ".join(titles),
        json.dumps(src_data.get("macro_narrative") or {}, ensure_ascii=False),
        json.dumps(src_data.get("macro_reasoning") or {}, ensure_ascii=False),
    ]
    return "\n".join(parts)


def _extract_confidence(src_data: dict) -> float:
    conf = src_data.get("confidence", 0.0)
    if isinstance(conf, dict):
        conf = conf.get("score", 0.0)
    try:
        conf_num = float(conf)
        if conf_num <= 1.0:
            conf_num *= 100.0
        return conf_num
    except Exception:
        return 50.0


def main() -> None:
    base = Path(__file__).resolve().parents[1]
    cfg = yaml.safe_load((base / "config.yaml").read_text(encoding="utf-8"))
    mb = cfg.get("macro_bridge", {})
    src = Path(mb.get("source_json", ""))
    if not src.exists():
        print(f"[SKIP] macro source missing: {src}")
        return

    src_data = json.loads(src.read_text(encoding="utf-8"))

    text_blob = _extract_text_blob(src_data, src)

    risk_hits = sum(
        1
        for k in ["risk", "warning", "冲突", "attack", "oil", "liquidity stress", "通胀", "衰退", "risk_off"]
        if _contains_any(text_blob, [k])
    )
    geo_hits = sum(1 for k in ["geopolitical", "hormuz", "中东", "制裁", "war", "冲突"] if _contains_any(text_blob, [k]))
    policy_tight_hits = sum(1 for k in ["tight", "紧缩", "hawkish", "加息", "缩表"] if _contains_any(text_blob, [k]))
    liquidity_ease_hits = sum(1 for k in ["宽松", "easing", "降息", "liquidity support", "刺激"] if _contains_any(text_blob, [k]))

    conf_num = _extract_confidence(src_data)

    macro_risk_score = _bounded(35 + risk_hits * 8)
    geopolitical_stress_score = _bounded(25 + geo_hits * 12)
    policy_tightening_score = _bounded(20 + policy_tight_hits * 14)
    global_liquidity_score = _bounded(45 + liquidity_ease_hits * 10 - policy_tight_hits * 6)
    event_confidence_score = _bounded(conf_num if conf_num > 0 else 50.0)

    out = {
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source_file": str(src),
        "features": {
            "macro_risk_score": round(macro_risk_score, 2),
            "geopolitical_stress_score": round(geopolitical_stress_score, 2),
            "policy_tightening_score": round(policy_tightening_score, 2),
            "global_liquidity_score": round(global_liquidity_score, 2),
            "event_confidence_score": round(event_confidence_score, 2),
        },
        "usage_contract": "宏观特征仅用于风险预算/仓位阈值，不直接替代交易信号。",
    }

    out_json = base / "reports" / "macro_features.json"
    out_md = base / "reports" / "macro_features.md"
    out_json.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [
        f"# 宏观特征映射（{out['updated_at']}）",
        "",
        f"- 来源：`{out['source_file']}`",
        "",
        "## 特征分数（0-100）",
    ]
    for k, v in out["features"].items():
        lines.append(f"- {k}: {v}")
    lines.extend(["", "## 使用约束", f"- {out['usage_contract']}"])
    out_md.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(f"[OK] wrote {out_json}")
    print(f"[OK] wrote {out_md}")


if __name__ == "__main__":
    main()
