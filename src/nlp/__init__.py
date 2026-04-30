from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Dict, Iterable, Optional


import requests


@dataclass
class LLMClientConfig:
    base_url: str
    api_key: str
    model: str
    timeout: int = 30

    @staticmethod
    def from_env() -> "LLMClientConfig":
        return LLMClientConfig(
            base_url=os.getenv("LLM_BASE_URL", "http://localhost:8317/v1"),
            api_key=os.getenv("LLM_API_KEY", "your-api-key-1"),
            model=os.getenv("LLM_MODEL", "gpt-5"),
            timeout=int(os.getenv("LLM_TIMEOUT", "30")),
        )


class OpenAICompatLLM:
    def __init__(self, config: Optional[LLMClientConfig] = None):
        self.config = config or LLMClientConfig.from_env()

    def chat(self, prompt: str, system: str = "你是资深量化研究助理") -> str:
        url = self.config.base_url.rstrip("/") + "/chat/completions"
        payload = {
            "model": self.config.model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": prompt},
            ],
            "temperature": 0.2,
        }
        headers = {
            "Authorization": f"Bearer {self.config.api_key}",
            "Content-Type": "application/json",
        }

        resp = requests.post(url, headers=headers, data=json.dumps(payload), timeout=self.config.timeout)
        resp.raise_for_status()
        data = resp.json()
        return data["choices"][0]["message"]["content"]


def score_sentiment_from_news(
    news_list: Iterable[str],
    llm: Optional[OpenAICompatLLM] = None,
) -> Dict[str, float]:
    """
    输入新闻文本列表，输出每条文本的情绪得分（-1 到 1）。
    当前用单次LLM调用做批量打分，后续可替换成更稳健的解析。
    """
    llm = llm or OpenAICompatLLM()
    items = list(news_list)
    if not items:
        return {}

    prompt = (
        "请对下面每条财经新闻做情绪打分，范围[-1,1]，仅返回JSON对象。"
        "键为序号字符串（从0开始），值为分数。\n"
    )
    for i, text in enumerate(items):
        prompt += f"{i}. {text}\n"

    raw = llm.chat(prompt=prompt)
    parsed = json.loads(raw)
    result: Dict[str, float] = {}
    for k, v in parsed.items():
        try:
            result[str(k)] = float(v)
        except Exception:
            continue
    return result
