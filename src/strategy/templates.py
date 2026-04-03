from __future__ import annotations

from typing import Dict


SIGNAL_TEMPLATES = {
    "momentum_core": {
        "lookbacks": [20, 60],
        "rebalance": "weekly",
        "risk_overlay": False,
        "position_style": "trend_following",
    },
    "momentum_plus_risk": {
        "lookbacks": [10, 20, 60],
        "rebalance": "weekly",
        "risk_overlay": True,
        "position_style": "fast_rotation",
    },
    "carry_defensive": {
        "lookbacks": [20],
        "rebalance": "biweekly",
        "risk_overlay": True,
        "position_style": "defensive_income",
    },
    "macro_sensitive": {
        "lookbacks": [10, 20, 40],
        "rebalance": "weekly",
        "risk_overlay": True,
        "position_style": "macro_shock",
    },
    "global_beta": {
        "lookbacks": [20, 60],
        "rebalance": "weekly",
        "risk_overlay": True,
        "position_style": "global_beta",
    },
}

MONITOR_TEMPLATES = {
    "broad_index_monitor": {"focus": ["trend", "breadth", "drawdown"], "cadence": "daily"},
    "sector_theme_monitor": {"focus": ["trend", "volatility", "crowding"], "cadence": "intraday+daily"},
    "bond_monitor": {"focus": ["yield", "drawdown", "carry"], "cadence": "daily"},
    "commodity_monitor": {"focus": ["macro", "volatility", "shock"], "cadence": "intraday+daily"},
    "cross_border_monitor": {"focus": ["global_market", "fx", "drawdown"], "cadence": "daily"},
}

BACKTEST_TEMPLATES = {
    "broad_index_backtest": {"fee_bps": 8, "slippage_bps": 6, "holding_limit": 5, "rebalance": "weekly"},
    "sector_theme_backtest": {"fee_bps": 10, "slippage_bps": 8, "holding_limit": 4, "rebalance": "weekly"},
    "bond_backtest": {"fee_bps": 5, "slippage_bps": 4, "holding_limit": 4, "rebalance": "biweekly"},
    "commodity_backtest": {"fee_bps": 12, "slippage_bps": 10, "holding_limit": 3, "rebalance": "weekly"},
    "cross_border_backtest": {"fee_bps": 12, "slippage_bps": 10, "holding_limit": 4, "rebalance": "weekly"},
}


def build_signal_template(name: str) -> Dict:
    return SIGNAL_TEMPLATES.get(name, SIGNAL_TEMPLATES["momentum_core"]).copy()


def build_monitor_template(name: str) -> Dict:
    return MONITOR_TEMPLATES.get(name, MONITOR_TEMPLATES["broad_index_monitor"]).copy()


def build_backtest_template(name: str) -> Dict:
    return BACKTEST_TEMPLATES.get(name, BACKTEST_TEMPLATES["broad_index_backtest"]).copy()


def build_class_bundle(strategy_template: str, monitor_template: str, backtest_template: str) -> Dict:
    return {
        "signal": build_signal_template(strategy_template),
        "monitor": build_monitor_template(monitor_template),
        "backtest": build_backtest_template(backtest_template),
    }
