from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Tuple

from src.nlp import OpenAICompatLLM

REPORT_DIR = Path(__file__).resolve().parents[2] / "reports"


def _extract_json_block(text: str) -> Dict:
    text = (text or "").strip()
    if not text:
        return {}

    try:
        return json.loads(text)
    except Exception:
        pass

    start = text.find("{")
    end = text.rfind("}")
    if start >= 0 and end > start:
        try:
            return json.loads(text[start : end + 1])
        except Exception:
            return {}
    return {}


def build_ai_research_review(context: Dict, llm: OpenAICompatLLM | None = None) -> Dict:
    llm = llm or OpenAICompatLLM()
    prompt = (
        "你是量化研究顾问。基于输入上下文，输出简洁客观的研究建议。"
        "只返回JSON，不要额外解释。\n"
        "JSON结构要求:"
        "{"
        '"overall_assessment": "一句话总结",'
        '"weaknesses": ["最多3条"],'
        '"overfit_risk": "low|medium|high",'
        '"next_actions": ["最多5条，偏可执行"],'
        '"risk_notes": ["最多3条"]'
        "}\n"
        "输入上下文如下:\n"
        f"{json.dumps(context, ensure_ascii=False)}"
    )

    try:
        raw = llm.chat(prompt=prompt, system="你是严谨的量化研究审阅助手")
    except Exception as e:
        return {
            "overall_assessment": "AI研究接口暂不可用，已回退为人工审阅模式。",
            "weaknesses": ["LLM调用失败，未生成自动建议"],
            "overfit_risk": "medium",
            "next_actions": ["检查 LLM_BASE_URL / LLM_API_KEY / LLM_MODEL", "先按风控与回测报告做人工复核"],
            "risk_notes": [f"LLM error: {e}"],
        }
    parsed = _extract_json_block(raw)
    if parsed:
        return parsed

    return {
        "overall_assessment": "LLM输出解析失败，建议人工查看原始报告。",
        "weaknesses": ["无法解析LLM JSON输出"],
        "overfit_risk": "medium",
        "next_actions": ["检查LLM接口返回格式", "人工复核回测与风控报告"],
        "risk_notes": ["AI研究结论不可直接自动执行"],
        "raw_text": raw,
    }


def _render_ai_review_markdown(review: Dict) -> str:
    weaknesses = review.get("weaknesses", []) or []
    actions = review.get("next_actions", []) or []
    risk_notes = review.get("risk_notes", []) or []

    lines = [
        "# AI 研究助手报告（仅建议，不自动执行）\n",
        f"- 总结: {review.get('overall_assessment', '')}",
        f"- 过拟合风险: {review.get('overfit_risk', 'medium')}\n",
        "## 主要弱点",
    ]
    lines.extend([f"- {x}" for x in weaknesses] or ["- 无"])
    lines.append("\n## 下一步建议")
    lines.extend([f"- {x}" for x in actions] or ["- 无"])
    lines.append("\n## 风险提示")
    lines.extend([f"- {x}" for x in risk_notes] or ["- AI建议需人工审核后执行"])
    return "\n".join(lines) + "\n"


def save_ai_research_review(review: Dict, prefix: str = "paper_rotation") -> Tuple[Path, Path]:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    json_path = REPORT_DIR / f"{prefix}_ai_review.json"
    md_path = REPORT_DIR / f"{prefix}_ai_review.md"

    json_path.write_text(json.dumps(review, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(_render_ai_review_markdown(review), encoding="utf-8")
    return json_path, md_path
