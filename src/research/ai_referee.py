from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

from src.nlp import OpenAICompatLLM

REPORT_DIR = Path(__file__).resolve().parents[2] / "reports"


def _extract_json(text: str):
    text = (text or "").strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except Exception:
        pass
    start = text.find("[")
    end = text.rfind("]")
    if start >= 0 and end > start:
        try:
            return json.loads(text[start : end + 1])
        except Exception:
            return None
    start = text.find("{")
    end = text.rfind("}")
    if start >= 0 and end > start:
        try:
            return json.loads(text[start : end + 1])
        except Exception:
            return None
    return None


def build_referee_feature_table(
    benchmark_close: pd.Series,
    rebalance: str = "W-FRI",
    mom_short: int = 20,
    mom_long: int = 60,
    vol_window: int = 20,
    dd_window: int = 60,
) -> pd.DataFrame:
    close = pd.Series(benchmark_close).sort_index().ffill().dropna()
    feat = pd.DataFrame(index=close.index)
    feat["close"] = close
    feat["mom_short"] = close.pct_change(mom_short)
    feat["mom_long"] = close.pct_change(mom_long)
    feat["vol"] = close.pct_change().rolling(vol_window).std()
    roll_max = close.rolling(dd_window).max()
    feat["drawdown"] = close / roll_max - 1.0
    out = feat.resample(rebalance).last().dropna(how="any")
    out.index.name = "date"
    return out


def _heuristic_label(row: pd.Series) -> Dict:
    mom_s = float(row.get("mom_short", 0.0))
    mom_l = float(row.get("mom_long", 0.0))
    vol = float(row.get("vol", 0.0))
    dd = float(row.get("drawdown", 0.0))

    score = 0.0
    score += 0.5 if mom_s > 0 else -0.5
    score += 0.5 if mom_l > 0 else -0.5
    score += 0.2 if dd > -0.08 else -0.2
    score += 0.2 if vol < 0.02 else -0.2
    score = max(-1.0, min(1.0, score / 1.4))

    if score >= 0.25:
        label = "bullish"
    elif score <= -0.25:
        label = "bearish"
    else:
        label = "neutral"

    conf = min(0.9, max(0.5, abs(score)))
    if vol >= 0.03 or dd <= -0.12:
        risk_tag = "high"
    elif vol >= 0.02 or dd <= -0.08:
        risk_tag = "medium"
    else:
        risk_tag = "low"

    return {
        "label": label,
        "confidence": round(float(conf), 4),
        "risk_tag": risk_tag,
        "score": round(float(score), 6),
        "source": "heuristic",
    }


def _llm_refine_recent_rows(rows: List[Dict], llm: OpenAICompatLLM) -> Dict[str, Dict]:
    if not rows:
        return {}
    prompt = (
        "你是量化风控裁判，只做风险倾向判断，不做价格预测。"
        "基于每条特征输出 label/confidence/risk_tag。"
        "label只能是 bullish/neutral/bearish；confidence范围[0,1]；risk_tag只能是 low/medium/high。"
        "只返回JSON数组，元素格式："
        "{\"date\":\"YYYY-MM-DD\",\"label\":\"bullish|neutral|bearish\",\"confidence\":0.73,\"risk_tag\":\"low|medium|high\"}。\n"
        f"输入特征: {json.dumps(rows, ensure_ascii=False)}"
    )
    raw = llm.chat(prompt=prompt, system="你是严谨的量化风险裁判")
    parsed = _extract_json(raw)
    if not isinstance(parsed, list):
        return {}

    out: Dict[str, Dict] = {}
    for item in parsed:
        if not isinstance(item, dict):
            continue
        dt = str(item.get("date", "")).strip()
        label = str(item.get("label", "neutral")).strip().lower()
        if label not in {"bullish", "neutral", "bearish"}:
            label = "neutral"
        try:
            confidence = float(item.get("confidence", 0.5))
        except Exception:
            confidence = 0.5
        confidence = float(max(0.0, min(1.0, confidence)))
        risk_tag = str(item.get("risk_tag", "medium")).strip().lower()
        if risk_tag not in {"low", "medium", "high"}:
            risk_tag = "medium"
        out[dt] = {
            "label": label,
            "confidence": round(confidence, 4),
            "risk_tag": risk_tag,
            "source": "llm",
        }
    return out


def _to_score(label: str, confidence: float, risk_tag: str) -> float:
    direction = {"bullish": 1.0, "neutral": 0.0, "bearish": -1.0}.get(label, 0.0)
    risk_mult = {"low": 1.0, "medium": 0.8, "high": 0.6}.get(risk_tag, 0.8)
    return float(max(-1.0, min(1.0, direction * confidence * risk_mult)))


def build_ai_referee_signals(
    benchmark_close: pd.Series,
    rebalance: str = "W-FRI",
    llm_enabled: bool = True,
    max_llm_points: int = 60,
) -> Tuple[pd.DataFrame, pd.Series]:
    feat = build_referee_feature_table(benchmark_close=benchmark_close, rebalance=rebalance)
    if feat.empty:
        return pd.DataFrame(), pd.Series(dtype=float)

    rows = []
    for dt, r in feat.iterrows():
        base = _heuristic_label(r)
        rows.append(
            {
                "date": pd.Timestamp(dt).strftime("%Y-%m-%d"),
                "mom_short": round(float(r.get("mom_short", 0.0)), 6),
                "mom_long": round(float(r.get("mom_long", 0.0)), 6),
                "vol": round(float(r.get("vol", 0.0)), 6),
                "drawdown": round(float(r.get("drawdown", 0.0)), 6),
                **base,
            }
        )

    llm_map: Dict[str, Dict] = {}
    if llm_enabled:
        try:
            llm = OpenAICompatLLM()
            tail_rows = [
                {
                    "date": x["date"],
                    "mom_short": x["mom_short"],
                    "mom_long": x["mom_long"],
                    "vol": x["vol"],
                    "drawdown": x["drawdown"],
                }
                for x in rows[-max(1, int(max_llm_points)) :]
            ]
            llm_map = _llm_refine_recent_rows(rows=tail_rows, llm=llm)
        except Exception:
            llm_map = {}

    out_rows = []
    for x in rows:
        override = llm_map.get(x["date"], {})
        label = str(override.get("label", x["label"]))
        confidence = float(override.get("confidence", x["confidence"]))
        risk_tag = str(override.get("risk_tag", x["risk_tag"]))
        source = str(override.get("source", x.get("source", "heuristic")))
        score = _to_score(label=label, confidence=confidence, risk_tag=risk_tag)
        out_rows.append(
            {
                "date": x["date"],
                "label": label,
                "confidence": confidence,
                "risk_tag": risk_tag,
                "score": score,
                "source": source,
                "mom_short": x["mom_short"],
                "mom_long": x["mom_long"],
                "vol": x["vol"],
                "drawdown": x["drawdown"],
            }
        )

    out_df = pd.DataFrame(out_rows)
    if out_df.empty:
        return out_df, pd.Series(dtype=float)
    sent = pd.Series(out_df["score"].values, index=pd.to_datetime(out_df["date"]), name="ai_referee_score")
    return out_df, sent


def save_ai_referee_outputs(df: pd.DataFrame, prefix: str = "paper_rotation") -> Tuple[Path, Path]:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = REPORT_DIR / f"{prefix}_ai_referee.csv"
    md_path = REPORT_DIR / f"{prefix}_ai_referee.md"
    if df is None or df.empty:
        pd.DataFrame(columns=["date", "label", "confidence", "risk_tag", "score", "source"]).to_csv(csv_path, index=False)
        md_path.write_text("# AI裁判输出\n\n- 无可用数据\n", encoding="utf-8")
        return csv_path, md_path

    df.to_csv(csv_path, index=False)
    recent = df.tail(12)
    lines = [
        "# AI裁判输出（仅作附加层）\n",
        f"- 总样本: {len(df)}",
        f"- 最近12期来源统计: {recent['source'].value_counts().to_dict()}",
        f"- 最近12期标签统计: {recent['label'].value_counts().to_dict()}\n",
        "| date | label | confidence | risk_tag | score | source |",
        "|---|---|---:|---|---:|---|",
    ]
    for _, r in recent.iterrows():
        lines.append(
            f"| {r['date']} | {r['label']} | {float(r['confidence']):.2f} | {r['risk_tag']} | {float(r['score']):.3f} | {r['source']} |"
        )
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return csv_path, md_path


def save_ab_compare(baseline_metrics: Dict, ai_metrics: Dict, prefix: str = "paper_rotation") -> Tuple[Path, Path, Path]:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = REPORT_DIR / f"{prefix}_ab_compare.csv"
    json_path = REPORT_DIR / f"{prefix}_ab_compare.json"
    md_path = REPORT_DIR / f"{prefix}_ab_compare.md"

    rows = []
    keys = ["annual_return", "sharpe", "max_drawdown", "alpha_annual", "cost_total", "win_rate"]
    for k in keys:
        b = float(baseline_metrics.get(k, 0.0))
        a = float(ai_metrics.get(k, 0.0))
        rows.append({"metric": k, "baseline": b, "ai_referee": a, "delta": a - b})
    df = pd.DataFrame(rows)
    df.to_csv(csv_path, index=False)

    better = int(((df["metric"].isin(["annual_return", "sharpe", "alpha_annual", "win_rate"])) & (df["delta"] > 0)).sum())
    better += int(((df["metric"] == "max_drawdown") & (df["delta"] > 0)).sum())
    better += int(((df["metric"] == "cost_total") & (df["delta"] < 0)).sum())
    decision = "AI_REFEREE_ON" if better >= 4 else "BASELINE_KEEP"

    payload = {"decision": decision, "better_score": better, "rows": rows}
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [
        "# A/B 对比（Baseline vs AI Referee）\n",
        f"- 决策: **{decision}**",
        f"- 优势计数: {better}\n",
        "| metric | baseline | ai_referee | delta |",
        "|---|---:|---:|---:|",
    ]
    for _, r in df.iterrows():
        lines.append(f"| {r['metric']} | {float(r['baseline']):.6f} | {float(r['ai_referee']):.6f} | {float(r['delta']):.6f} |")
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    return csv_path, json_path, md_path
