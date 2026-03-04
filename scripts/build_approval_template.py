import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / "reports"


def main():
    rec_path = REPORTS / "paper_rotation_research_recommendation.json"
    out_path = REPORTS / "paper_rotation_approved_params_template.json"

    if not rec_path.exists():
        print(f"[warn] recommendation not found: {rec_path}")
        return

    rec = json.loads(rec_path.read_text(encoding="utf-8"))
    candidate = rec.get("candidate_params") or {}

    base = {
        "top_n": int(candidate.get("top_n", 2)),
        "min_score": float(candidate.get("min_score", -0.1)),
        "vol_lookback": int(candidate.get("vol_lookback", 20)),
        "max_turnover": 0.8,
        "target_vol_ann": 0.12,
        "drawdown_stop": -0.05,
        "dd_cooldown_days": 5,
        "fee_bps": 5.0,
        "slippage_bps": 5.0,
        "impact_bps": 2.0,
        "impact_power": 0.5,
        "impact_bps_cap_mult": 5.0,
        "daily_loss_stop": -0.03,
        "monthly_drawdown_stop": -0.10,
        "stop_cooldown_days": 3,
        "regime_filter_enabled": True,
        "regime_ma_window": 200,
        "regime_vol_window": 20,
        "regime_high_vol_threshold": 0.02,
        "regime_defensive_exposure": 0.30,
        "max_daily_drawdown": -0.05,
        "max_total_drawdown": -0.15,
        "max_position_weight": 0.60,
        "_note": "复制为 paper_rotation_approved_params.json 后生效；请人工复核后再启用。参数冻结默认90天，强制覆盖请运行 apply_approved_template.py --force",
    }

    out_path.write_text(json.dumps(base, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"saved: {out_path}")


if __name__ == "__main__":
    main()
