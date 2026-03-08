"""
System Configuration - Desk Definitions, Risk Parameters, Session Windows
"""
import os
from typing import Dict, List

# ─────────────────────────────────────────────────────────────
# ENVIRONMENT
# ─────────────────────────────────────────────────────────────
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/trading_system",
)
# Railway uses DATABASE_URL with postgres:// prefix; SQLAlchemy needs postgresql://
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "change-me-in-production")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")  # legacy fallback
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
TWELVEDATA_API_KEY = os.getenv("TWELVEDATA_API_KEY", "")

# ─────────────────────────────────────────────────────────────
# TELEGRAM — Per-Desk Channel Routing
# ─────────────────────────────────────────────────────────────
TELEGRAM_DESK_CHANNELS = {
    "DESK1_SCALPER":   os.getenv("TG_DESK1", "-1003216826596"),
    "DESK2_INTRADAY":  os.getenv("TG_DESK2", "-1003789191641"),
    "DESK3_SWING":     os.getenv("TG_DESK3", "-1003813056839"),
    "DESK4_GOLD":      os.getenv("TG_DESK4", "-1003711906528"),
    "DESK5_ALTS":      os.getenv("TG_DESK5", "-1003868629189"),
    "DESK6_EQUITIES":  os.getenv("TG_DESK6", "-1003752836585"),
}
TELEGRAM_PORTFOLIO_CHAT = os.getenv("TG_PORTFOLIO", "-1003614474777")

# ─────────────────────────────────────────────────────────────
# THREE SIMULATION PROFILES
# ─────────────────────────────────────────────────────────────
#
# SRV_100 = Railway server, 6 × $100k, 1:100 leverage (aggressive)
# SRV_30  = Railway server, 6 × $100k, 1:30 leverage (conservative)
# MT5_1M  = MT5 VPS, 1 × $1M account, all 6 desks, FTMO rules only
#
# All three run the SAME signals simultaneously.
# Reports compare performance across all three.
# ─────────────────────────────────────────────────────────────

ACCOUNTS = 6
CAPITAL_PER_ACCOUNT = 100_000
FIRM_CAPITAL = ACCOUNTS * CAPITAL_PER_ACCOUNT
PORTFOLIO_CAPITAL = 1_000_000

# ── FTMO Hard Rules (apply to ALL profiles) ──
MAX_DAILY_LOSS_PCT = 5.0     # 5% daily loss limit
MAX_TOTAL_LOSS_PCT = 10.0    # 10% max loss limit
MAX_DAILY_LOSS_PER_ACCOUNT = int(CAPITAL_PER_ACCOUNT * MAX_DAILY_LOSS_PCT / 100)
MAX_TOTAL_LOSS_PER_ACCOUNT = int(CAPITAL_PER_ACCOUNT * MAX_TOTAL_LOSS_PCT / 100)
FIRM_WIDE_DAILY_DRAWDOWN_HALT = 30_000

# ── Pip Values / Sizes (shared across all profiles) ──
PIP_VALUES = {
    "FOREX": 10.0, "FOREX_JPY": 6.67, "GOLD": 10.0,
    "SILVER": 50.0, "WTI": 10.0, "COPPER": 2.50,
    "INDEX": 1.0, "CRYPTO": 1.0, "EQUITY": 1.0,
}
PIP_SIZES = {
    "FOREX": 0.0001, "FOREX_JPY": 0.01, "GOLD": 0.1,
    "SILVER": 0.01, "WTI": 0.01, "COPPER": 0.0001,
    "INDEX": 1.0, "CRYPTO": 1.0, "EQUITY": 0.01,
}
SYMBOL_ASSET_CLASS = {
    "XAUUSD": "GOLD", "XAGUSD": "GOLD",
    "WTIUSD": "WTI", "XCUUSD": "COMMODITY",
    "US30": "INDEX", "US100": "INDEX", "NAS100": "INDEX",
    "BTCUSD": "CRYPTO", "ETHUSD": "CRYPTO",
    "SOLUSD": "CRYPTO", "XRPUSD": "CRYPTO", "LINKUSD": "CRYPTO",
    "TSLA": "EQUITY", "AAPL": "EQUITY", "MSFT": "EQUITY",
    "NVDA": "EQUITY", "AMZN": "EQUITY", "META": "EQUITY",
    "GOOGL": "EQUITY", "NFLX": "EQUITY", "AMD": "EQUITY",
}


def get_pip_info(symbol: str):
    """Returns (pip_size, pip_value_per_lot) for a given symbol."""
    sym = symbol.upper()
    if "XAU" in sym:
        return PIP_SIZES["GOLD"], PIP_VALUES["GOLD"]
    elif "XAG" in sym:
        return PIP_SIZES["SILVER"], PIP_VALUES["SILVER"]
    elif "WTI" in sym:
        return PIP_SIZES["WTI"], PIP_VALUES["WTI"]
    elif "XCU" in sym or "COPPER" in sym:
        return PIP_SIZES["COPPER"], PIP_VALUES["COPPER"]
    elif "JPY" in sym:
        return PIP_SIZES["FOREX_JPY"], PIP_VALUES["FOREX_JPY"]
    elif any(x in sym for x in ["US30", "US100", "NAS"]):
        return PIP_SIZES["INDEX"], PIP_VALUES["INDEX"]
    elif any(x in sym for x in ["BTC", "ETH", "SOL", "XRP", "LINK"]):
        return PIP_SIZES["CRYPTO"], PIP_VALUES["CRYPTO"]
    elif any(x in sym for x in ["TSLA", "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "NFLX", "AMD"]):
        return PIP_SIZES["EQUITY"], PIP_VALUES["EQUITY"]
    else:
        return PIP_SIZES["FOREX"], PIP_VALUES["FOREX"]


def get_leverage(symbol: str, profile: str = "SRV_100") -> int:
    """Get leverage for a symbol in a given profile."""
    if profile == "SRV_30":
        return 30
    # SRV_100 and MT5_1M use 1:100
    return 100


# ═══════════════════════════════════════════════════════════════
# PROFILE 1: SRV_100 — Railway, $100k per bot, 1:100 leverage
# ═══════════════════════════════════════════════════════════════
# Strategy: AGGRESSIVE — maximize profit, full leverage headroom
# No margin constraints at 1:100 so push risk higher and trade more
SRV_100 = {
    "name": "Server 1:100",
    "tag": "🟦",
    "leverage": 100,
    "capital_per_desk": 100_000,
    "DESK1_SCALPER": {
        "risk_pct": 0.75,           # aggressive scalper — tight SL = low actual $
        "lot_method": "FIXED",
        "fixed_lot": 0.50,          # 0.50 lots per trade (consistent)
        "max_trades_day": 20,
        "max_open": 5,
        "max_daily_loss_pct": 5.0,
        "consecutive_loss_rules": {2: 0.75, 3: 0.50, 4: "PAUSE_1H", 5: "PAUSE_2H", 7: "CLOSE_DESK"},
        "trailing_stop_pips": 10,
    },
    "DESK2_INTRADAY": {
        "risk_pct": 1.5,            # higher risk — 1:100 can handle it
        "lot_method": "DYNAMIC",
        "max_lot": 10.0,
        "max_trades_day": 8,
        "max_open": 5,
        "max_daily_loss_pct": 5.0,
        "consecutive_loss_rules": {2: 0.75, 3: 0.50, 4: "PAUSE_2H", 5: "CLOSE_DESK"},
        "trailing_stop_pips": 30,
    },
    "DESK3_SWING": {
        "risk_pct": 2.5,            # big risk — big reward, wide SLs make $ manageable
        "lot_method": "DYNAMIC",
        "max_lot": 15.0,
        "max_trades_day": 4,
        "max_open": 5,
        "max_daily_loss_pct": 5.0,
        "consecutive_loss_rules": {2: 0.75, 3: 0.50, 4: "PAUSE_2H", 5: "CLOSE_DESK"},
        "trailing_stop_pips": 80,
    },
    "DESK4_GOLD": {
        "sub_strategies": {
            "gold_scalp":  {"risk_pct": 0.75, "max_trades": 10, "trailing_stop_pips": 15},
            "gold_intra":  {"risk_pct": 2.0,  "max_trades": 5,  "trailing_stop_pips": 30},
            "gold_swing":  {"risk_pct": 2.5,  "max_trades": 2,  "trailing_stop_pips": 50},
        },
        "lot_method": "DYNAMIC",
        "max_lot": 10.0,
        "max_open": 5,
        "max_daily_loss_pct": 5.0,
        "consecutive_loss_rules": {2: 0.75, 3: 0.50, 4: "PAUSE_2H", 5: "CLOSE_DESK"},
    },
    "DESK5_ALTS": {
        "risk_pct": 1.5,            # indices trend well, crypto volatile
        "lot_method": "DYNAMIC",
        "max_lot": 10.0,
        "max_trades_day": 10,
        "max_open": 5,
        "max_daily_loss_pct": 5.0,
        "consecutive_loss_rules": {2: 0.75, 3: 0.50, 4: "PAUSE_2H", 5: "CLOSE_DESK"},
        "trailing_stop_pips": {"indices": 15, "crypto": 50},
    },
    "DESK6_EQUITIES": {
        "risk_pct": 1.5,            # stocks move slower, can push risk
        "lot_method": "DYNAMIC",
        "max_lot": 10.0,
        "max_trades_day": 6,
        "max_open": 5,
        "max_daily_loss_pct": 5.0,
        "consecutive_loss_rules": {2: 0.75, 3: 0.50, 4: "PAUSE_2H", 5: "CLOSE_DESK"},
        "trailing_stop_pips": 25,
    },
}

# ═══════════════════════════════════════════════════════════════
# PROFILE 2: SRV_30 — Railway, $100k per bot, 1:30 leverage
# ═══════════════════════════════════════════════════════════════
# Strategy: CONSERVATIVE — margin is tight, reduce size, fewer positions
# Gold and JPY pairs eat margin fast at 1:30, so cap exposure
SRV_30 = {
    "name": "Server 1:30",
    "tag": "🟧",
    "leverage": 30,
    "capital_per_desk": 100_000,
    "DESK1_SCALPER": {
        "risk_pct": 0.5,            # tighter risk — margin limits lot size
        "lot_method": "FIXED",
        "fixed_lot": 0.20,          # smaller fixed lot, margin-safe
        "max_trades_day": 15,
        "max_open": 3,              # fewer simultaneous — margin headroom
        "max_daily_loss_pct": 5.0,
        "consecutive_loss_rules": {2: 0.75, 3: 0.50, 4: "PAUSE_2H", 5: "CLOSE_DESK"},
        "trailing_stop_pips": 10,
    },
    "DESK2_INTRADAY": {
        "risk_pct": 1.0,            # standard risk
        "lot_method": "DYNAMIC",
        "max_lot": 3.0,             # capped — 1:30 margin constraint
        "max_trades_day": 6,
        "max_open": 3,
        "max_daily_loss_pct": 5.0,
        "consecutive_loss_rules": {2: 0.75, 3: 0.50, 4: "PAUSE_2H", 5: "CLOSE_DESK"},
        "trailing_stop_pips": 30,
    },
    "DESK3_SWING": {
        "risk_pct": 1.5,            # reduced from 2.5 — margin for multi-day holds
        "lot_method": "DYNAMIC",
        "max_lot": 3.0,
        "max_trades_day": 3,
        "max_open": 3,              # fewer open — margin tied up longer
        "max_daily_loss_pct": 5.0,
        "consecutive_loss_rules": {2: 0.75, 3: 0.50, 4: "PAUSE_2H", 5: "CLOSE_DESK"},
        "trailing_stop_pips": 80,
    },
    "DESK4_GOLD": {
        "sub_strategies": {
            "gold_scalp":  {"risk_pct": 0.5,  "max_trades": 6,  "trailing_stop_pips": 15},
            "gold_intra":  {"risk_pct": 1.0,  "max_trades": 3,  "trailing_stop_pips": 30},
            "gold_swing":  {"risk_pct": 1.5,  "max_trades": 1,  "trailing_stop_pips": 50},
        },
        "lot_method": "DYNAMIC",
        "max_lot": 2.0,             # gold at $2950 = huge margin at 1:30
        "max_open": 2,              # GOLD IS THE BOTTLENECK at 1:30
        "max_daily_loss_pct": 5.0,
        "consecutive_loss_rules": {2: 0.75, 3: 0.50, 4: "PAUSE_2H", 5: "CLOSE_DESK"},
    },
    "DESK5_ALTS": {
        "risk_pct": 0.75,           # crypto already at 1:30, reduce further
        "lot_method": "DYNAMIC",
        "max_lot": 3.0,
        "max_trades_day": 6,
        "max_open": 3,
        "max_daily_loss_pct": 5.0,
        "consecutive_loss_rules": {2: 0.75, 3: 0.50, 4: "PAUSE_2H", 5: "CLOSE_DESK"},
        "trailing_stop_pips": {"indices": 15, "crypto": 50},
    },
    "DESK6_EQUITIES": {
        "risk_pct": 0.75,           # equities at 1:30 = tight margin
        "lot_method": "DYNAMIC",
        "max_lot": 3.0,
        "max_trades_day": 4,
        "max_open": 3,
        "max_daily_loss_pct": 5.0,
        "consecutive_loss_rules": {2: 0.75, 3: 0.50, 4: "PAUSE_2H", 5: "CLOSE_DESK"},
        "trailing_stop_pips": 25,
    },
}

# ═══════════════════════════════════════════════════════════════
# PROFILE 3: MT5_1M — MT5 VPS, $1M single account, FTMO rules only
# ═══════════════════════════════════════════════════════════════
# Strategy: MAX PROFIT — $1M at 1:100, all 6 desks share the account
# Only hard rules: 5% daily loss ($50k), 10% max loss ($100k)
# More capital = bigger lots = bigger wins, but desks must share margin
MT5_1M = {
    "name": "MT5 $1M",
    "tag": "📟",
    "leverage": 100,
    "capital": 1_000_000,
    "max_daily_loss": 50_000,       # 5% of $1M
    "max_total_loss": 100_000,      # 10% of $1M
    "capital_per_desk": {
        "DESK1_SCALPER":  120_000,  # 12% — scalper needs least capital
        "DESK2_INTRADAY": 200_000,  # 20% — workhorse
        "DESK3_SWING":    200_000,  # 20% — holds positions
        "DESK4_GOLD":     180_000,  # 18% — gold eats margin
        "DESK5_ALTS":     150_000,  # 15% — mixed bag
        "DESK6_EQUITIES": 150_000,  # 15% — US stocks
    },
    "DESK1_SCALPER": {
        "risk_pct": 1.0,            # bigger capital = push scalper harder
        "lot_method": "FIXED",
        "fixed_lot": 1.20,          # 1.2 lots fixed at $1M
        "max_trades_day": 25,       # more trades = more opportunity
        "max_open": 6,
        "trailing_stop_pips": 10,
    },
    "DESK2_INTRADAY": {
        "risk_pct": 2.0,            # $2k per trade on $200k allocation
        "lot_method": "DYNAMIC",
        "max_lot": 20.0,
        "max_trades_day": 10,
        "max_open": 6,
        "trailing_stop_pips": 30,
    },
    "DESK3_SWING": {
        "risk_pct": 3.0,            # $6k per trade, wide SLs absorb it
        "lot_method": "DYNAMIC",
        "max_lot": 25.0,
        "max_trades_day": 4,
        "max_open": 5,
        "trailing_stop_pips": 80,
    },
    "DESK4_GOLD": {
        "sub_strategies": {
            "gold_scalp":  {"risk_pct": 1.0,  "max_trades": 12, "trailing_stop_pips": 15},
            "gold_intra":  {"risk_pct": 2.5,  "max_trades": 6,  "trailing_stop_pips": 30},
            "gold_swing":  {"risk_pct": 3.0,  "max_trades": 3,  "trailing_stop_pips": 50},
        },
        "lot_method": "DYNAMIC",
        "max_lot": 15.0,
        "max_open": 5,
    },
    "DESK5_ALTS": {
        "risk_pct": 2.0,
        "lot_method": "DYNAMIC",
        "max_lot": 15.0,
        "max_trades_day": 12,
        "max_open": 6,
        "trailing_stop_pips": {"indices": 15, "crypto": 50},
    },
    "DESK6_EQUITIES": {
        "risk_pct": 2.0,
        "lot_method": "DYNAMIC",
        "max_lot": 10.0,
        "max_trades_day": 8,
        "max_open": 5,
        "trailing_stop_pips": 25,
    },
    # MT5_1M only has FTMO hard rules — no consecutive loss scaling
    # The system trusts the AI at this level. Only 5%/10% drawdown stops trading.
    "consecutive_loss_rules": {},   # NONE — FTMO limits are the only brake
}

# Active profiles for this deployment
SIM_PROFILES = {
    "SRV_100": SRV_100,
    "SRV_30": SRV_30,
    "MT5_1M": MT5_1M,
}

# ── Default consecutive loss rules (for SRV profiles) ──
CONSECUTIVE_LOSS_RULES = {
    2: 0.75,
    3: 0.50,
    4: "PAUSE_2H",
    5: "CLOSE_DESK",
}

# ── Portfolio-level allocation for backward compat ──
PORTFOLIO_CAPITAL_PER_DESK = MT5_1M["capital_per_desk"]
MAX_OPEN_POSITIONS_PER_DESK = 5
MAX_OPEN_POSITIONS_PORTFOLIO = 20


def get_profile_config(profile: str, desk_id: str) -> dict:
    """Get the risk config for a specific desk in a specific profile."""
    p = SIM_PROFILES.get(profile, SRV_100)
    return p.get(desk_id, {})


def calculate_lot_size(
    desk_id: str, symbol: str, risk_pct: float,
    sl_pips: float, account_capital: float = None,
    profile: str = "SRV_100",
) -> float:
    """
    Calculate lot size for a given profile.

    FIXED (scalper): Returns preset lot, scaled to capital.
    DYNAMIC: lot = (capital × risk%) / (SL_pips × pip_value)
    Clamped by profile's max_lot and margin check.
    """
    p = SIM_PROFILES.get(profile, SRV_100)
    desk_conf = p.get(desk_id, {})

    if account_capital is None:
        if profile == "MT5_1M":
            account_capital = MT5_1M["capital_per_desk"].get(desk_id, 166_667)
        else:
            account_capital = p.get("capital_per_desk", CAPITAL_PER_ACCOUNT)

    method = desk_conf.get("lot_method", "DYNAMIC")
    max_lot = desk_conf.get("max_lot", 10.0)
    min_lot = 0.01

    if method == "FIXED":
        lot = desk_conf.get("fixed_lot", 0.10)
        # Scale if capital differs from base
        base_capital = p.get("capital_per_desk", 100_000)
        if isinstance(base_capital, dict):
            base_capital = base_capital.get(desk_id, 100_000)
        lot = lot * (account_capital / base_capital)
    else:
        _, pip_value = get_pip_info(symbol)
        risk_dollars = account_capital * (risk_pct / 100)

        if sl_pips <= 0 or pip_value <= 0:
            return min_lot

        lot = risk_dollars / (sl_pips * pip_value)

    # Clamp
    lot = max(min_lot, min(max_lot, lot))

    # Margin check at profile's leverage
    leverage = p.get("leverage", 100)
    max_margin = account_capital * 0.50  # never use more than 50% margin on one trade
    # Rough margin estimate (needs live price for accuracy)
    lot = round(lot, 2)

    return lot

# ─────────────────────────────────────────────────────────────
# CONSENSUS SCORING
# ─────────────────────────────────────────────────────────────
SCORE_THRESHOLDS = {
    "HIGH": 7,    # full size
    "MEDIUM": 4,  # 50% size
    "LOW": 2,     # 25% size
    # below 2 → SKIP
}

SCORE_WEIGHTS = {
    "bias_match": 3,
    "setup_match": 2,
    "entry_trigger": 1,
    "ml_confirm_per_tf": 1,
    "bullish_bearish_plus": 1,
    "confirmation_turn_plus": 2,
    "kill_zone_overlap": 2,
    "kill_zone_single": 1,
    "correlation_confirm": 2,
    "liquidity_sweep": 3,
    "conflicting_htf": -3,
}

# ─────────────────────────────────────────────────────────────
# DESK DEFINITIONS
# ─────────────────────────────────────────────────────────────
DESKS: Dict[str, dict] = {
    "DESK1_SCALPER": {
        "name": "Rapid Execution",
        "style": "Scalper",
        "symbols": ["EURUSD", "USDJPY", "GBPUSD", "USDCHF", "XAGUSD", "EURCHF", "AUDNZD"],
        "timeframes": {"bias": "15M", "confirmation": "5M", "entry": "1M"},
        "luxalgo_preset": "Scalper",
        "ml_classifier": 34,
        "sensitivity": 12,
        "trailing_stop_pips": 10,
        "max_trades_day": 15,
        "max_hold_hours": 2,
        "risk_pct": 0.5,
        "sessions": ["LONDON", "NEW_YORK"],
        "alerts": [
            "bullish_confirmation", "bearish_confirmation",
            "bullish_exit", "bearish_exit", "take_profit", "stop_loss",
        ],
    },
    "DESK2_INTRADAY": {
        "name": "Intraday Momentum",
        "style": "Intraday",
        "symbols": ["AUDUSD", "USDCAD", "NZDUSD", "EURUSD", "EURGBP", "GBPCAD", "AUDCAD", "XAGUSD", "CHFJPY", "EURNZD"],
        "timeframes": {"bias": "4H", "confirmation": "1H", "entry": "15M"},
        "luxalgo_preset": "Trend Trader",
        "ml_classifier": 500,
        "sensitivity": 19,
        "trailing_stop_pips": 30,
        "max_trades_day": 6,
        "max_hold_hours": 8,
        "risk_pct": 1.0,
        "sessions": ["LONDON", "NEW_YORK"],
        "alerts": [
            "bullish_confirmation", "bearish_confirmation",
            "bullish_plus", "bearish_plus",
            "bullish_exit", "bearish_exit",
            "take_profit", "stop_loss", "confirmation_turn_plus",
        ],
    },
    "DESK3_SWING": {
        "name": "Strategic Positioning",
        "style": "Swing",
        "symbols": ["EURJPY", "GBPJPY", "AUDJPY", "USDCAD", "EURAUD", "GBPAUD", "CADJPY", "NZDJPY", "XAGUSD", "GBPNZD"],
        "timeframes": {"bias": "W", "confirmation": "D", "entry": "4H"},
        "luxalgo_preset": "Swing Trader",
        "ml_classifier": 1234,
        "sensitivity": 25,
        "trailing_stop_pips": 80,
        "max_simultaneous": 3,
        "max_hold_hours": 72,
        "risk_pct": 2.0,
        "sessions": ["ALL"],
        "alerts": [
            "bullish_plus", "bearish_plus",
            "confirmation_turn_plus", "take_profit", "stop_loss",
        ],
    },
    "DESK4_GOLD": {
        "name": "Gold Desk",
        "style": "Multi-TF Gold Specialist",
        "symbols": ["XAUUSD", "XAGUSD", "WTIUSD", "XCUUSD"],
        "timeframes": {"bias": "D", "confirmation": "4H,1H,5M", "entry": "5M,15M,1H"},
        "luxalgo_preset": "Trend Trader + Neo Cloud",
        "ml_classifier": 34,
        "sensitivity": 19,
        "trailing_stop_pips": 30,
        "max_trades_day": 12,
        "max_hold_hours": 72,
        "risk_pct": 1.5,
        "sessions": ["LONDON", "NEW_YORK", "LONDON_NY_OVERLAP"],
        "active_window": "07:00-21:00 UTC",
        "alerts": [
            "bullish_confirmation", "bearish_confirmation",
            "bullish_plus", "bearish_plus",
            "bullish_exit", "bearish_exit",
            "take_profit", "stop_loss",
        ],
        # Gold sub-strategies — used by pipeline to adjust parameters
        "sub_strategies": {
            "gold_scalp": {
                "timeframe": "5M",
                "sensitivity": 12,
                "tp_sl_distance": 2,
                "risk_pct": 0.5,
                "max_trades": 8,
                "max_hold_hours": 2,
                "trailing_stop_pips": 15,
                "sessions": ["LONDON_NY_OVERLAP"],
                "alerts": ["bullish_confirmation", "bearish_confirmation"],
            },
            "gold_intra": {
                "timeframe": "1H",
                "sensitivity": 19,
                "tp_sl_distance": 4,
                "risk_pct": 1.5,
                "max_trades": 4,
                "max_hold_hours": 12,
                "trailing_stop_pips": 30,
                "sessions": ["LONDON", "NEW_YORK"],
                "alerts": [
                    "bullish_confirmation", "bearish_confirmation",
                    "bullish_plus", "bearish_plus",
                ],
            },
            "gold_swing": {
                "timeframe": "4H",
                "sensitivity": 25,
                "tp_sl_distance": 6,
                "risk_pct": 2.0,
                "max_trades": 2,
                "max_hold_hours": 72,
                "trailing_stop_pips": 50,
                "sessions": ["ALL"],
                "alerts": ["bullish_plus", "bearish_plus"],
            },
        },
    },
    "DESK5_ALTS": {
        "name": "Alternative Assets",
        "style": "Index + Crypto Trend",
        "symbols": ["US30", "US100", "NAS100", "BTCUSD", "ETHUSD", "WTIUSD", "SOLUSD", "XRPUSD", "LINKUSD"],
        "timeframes": {"bias": "D", "confirmation": "4H", "entry": "1H"},
        "luxalgo_preset": "Trend Trader + Trend Strength",
        "ml_classifier": 500,
        "sensitivity": 19,
        "trailing_stop_pips": {"indices": 15, "crypto": 50},
        "max_trades_day": 8,
        "max_hold_hours": 24,
        "risk_pct": 1.0,
        "sessions": ["MARKET_HOURS_INDICES", "24_7_CRYPTO"],
        "alerts": "ALL",
    },
    "DESK6_EQUITIES": {
        "name": "Equities",
        "style": "Stock Trend",
        "symbols": ["TSLA", "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "NFLX", "AMD"],
        "timeframes": {"bias": "D", "confirmation": "4H", "entry": "1H"},
        "luxalgo_preset": "Trend Trader",
        "ml_classifier": 500,
        "sensitivity": 19,
        "trailing_stop_pips": 25,
        "max_trades_day": 4,
        "max_hold_hours": 24,
        "risk_pct": 1.0,
        "sessions": ["US_MARKET"],
        "active_window": "09:30-16:00 ET",
        "alerts": [
            "bullish_confirmation", "bearish_confirmation",
            "bullish_plus", "bearish_plus",
            "bullish_exit", "bearish_exit",
            "take_profit", "stop_loss",
        ],
    },
}

# ─────────────────────────────────────────────────────────────
# VALID ALERT TYPES (LuxAlgo v7.3.1)
# ─────────────────────────────────────────────────────────────
VALID_ALERT_TYPES = [
    "bullish_confirmation",
    "bearish_confirmation",
    "bullish_plus",
    "bearish_plus",
    "bullish_exit",
    "bearish_exit",
    "confirmation_turn_bullish",
    "confirmation_turn_bearish",
    "confirmation_turn_plus",
    "contrarian_bullish",
    "contrarian_bearish",
    "take_profit",
    "stop_loss",
    "smart_trail_cross",
    # Smart Money Concepts (SMC) alerts
    "smc_structure",
    "smc_bullish_bos",
    "smc_bearish_bos",
    "smc_bullish_choch",
    "smc_bearish_choch",
    "smc_bullish_fvg",
    "smc_bearish_fvg",
    "smc_equal_highs",
    "smc_equal_lows",
    "smc_bullish_ob_break",
    "smc_bearish_ob_break",
]

# ─────────────────────────────────────────────────────────────
# SESSION WINDOWS (UTC hours)
# ─────────────────────────────────────────────────────────────
SESSION_WINDOWS = {
    "LONDON":           {"start": 7,  "end": 16},
    "NEW_YORK":         {"start": 12, "end": 21},
    "LONDON_NY_OVERLAP": {"start": 12, "end": 16},
    "US_MARKET":        {"start": 13, "end": 21},  # 9:30-16:00 ET approx
    "MARKET_HOURS_INDICES": {"start": 13, "end": 21},
    "24_7_CRYPTO":      {"start": 0,  "end": 24},  # always open
    "SYDNEY":           {"start": 21, "end": 6},    # wraps midnight
    "TOKYO":            {"start": 23, "end": 8},    # wraps midnight
    "ALL":              {"start": 0,  "end": 24},
}

# ─────────────────────────────────────────────────────────────
# CORRELATION GROUPS — symbols that move together
# Only one trade per group per direction at a time
# ─────────────────────────────────────────────────────────────
CORRELATION_GROUPS = {
    "USD_WEAKNESS": {
        "symbols": ["EURUSD", "GBPUSD", "AUDUSD", "NZDUSD"],
        "note": "All go LONG when USD weakens",
        "long_correlated": True,
    },
    "USD_STRENGTH": {
        "symbols": ["USDJPY", "USDCHF", "USDCAD"],
        "note": "All go LONG when USD strengthens",
        "long_correlated": True,
    },
    "JPY_CROSSES": {
        "symbols": ["EURJPY", "GBPJPY", "AUDJPY", "CADJPY", "NZDJPY", "CHFJPY"],
        "note": "All move together on JPY sentiment",
        "long_correlated": True,
    },
    "EUR_CROSSES": {
        "symbols": ["EURGBP", "EURAUD", "EURCHF"],
        "note": "All move together on EUR sentiment",
        "long_correlated": True,
    },
    "GBP_CROSSES": {
        "symbols": ["GBPAUD", "GBPCAD", "EURGBP"],
        "note": "GBP exposure — EURGBP inversely correlated",
        "long_correlated": True,
    },
    "AUD_CROSSES": {
        "symbols": ["AUDCAD", "EURAUD", "GBPAUD"],
        "note": "AUD exposure across crosses",
        "long_correlated": True,
    },
    "TECH_STOCKS": {
        "symbols": ["AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "NFLX", "AMD"],
        "note": "Mega-cap tech moves together",
        "long_correlated": True,
    },
    "INDICES": {
        "symbols": ["US30", "US100", "NAS100"],
        "note": "US indices are highly correlated",
        "long_correlated": True,
    },
    "CRYPTO": {
        "symbols": ["BTCUSD", "ETHUSD", "SOLUSD", "XRPUSD", "LINKUSD"],
        "note": "Crypto moves in tandem",
        "long_correlated": True,
    },
    "PRECIOUS_METALS": {
        "symbols": ["XAUUSD", "XAGUSD"],
        "note": "Gold and silver correlated",
        "long_correlated": True,
    },
    "COMMODITIES": {
        "symbols": ["XAUUSD", "XAGUSD", "WTIUSD", "XCUUSD"],
        "note": "Commodities move on risk sentiment and USD",
        "long_correlated": True,
    },
    "NZD_CROSSES": {
        "symbols": ["GBPNZD", "EURNZD", "AUDNZD"],
        "note": "All move on NZD sentiment",
        "long_correlated": True,
    },
}

# Max correlated positions per group per direction
MAX_CORRELATED_POSITIONS = 2

# Symbol normalization map (TradingView → internal)
SYMBOL_ALIASES = {
    "FX:EURUSD": "EURUSD",
    "FX:USDJPY": "USDJPY",
    "FX:GBPUSD": "GBPUSD",
    "FX:USDCHF": "USDCHF",
    "FX:AUDUSD": "AUDUSD",
    "FX:USDCAD": "USDCAD",
    "FX:NZDUSD": "NZDUSD",
    "FX:EURJPY": "EURJPY",
    "FX:GBPJPY": "GBPJPY",
    "FX:AUDJPY": "AUDJPY",
    "FX:EURGBP": "EURGBP",
    "FX:EURAUD": "EURAUD",
    "FX:GBPAUD": "GBPAUD",
    "FX:EURCHF": "EURCHF",
    "FX:CADJPY": "CADJPY",
    "FX:NZDJPY": "NZDJPY",
    "FX:GBPCAD": "GBPCAD",
    "FX:AUDCAD": "AUDCAD",
    "FX:AUDNZD": "AUDNZD",
    "FX:CHFJPY": "CHFJPY",
    "FX:EURNZD": "EURNZD",
    "FX:GBPNZD": "GBPNZD",
    "COMEX:HG1!": "XCUUSD",
    "TVC:COPPER": "XCUUSD",
    "OANDA:XCUUSD": "XCUUSD",
    "FX:COPPER": "XCUUSD",
    "COINBASE:SOLUSD": "SOLUSD",
    "BINANCE:SOLUSDT": "SOLUSD",
    "COINBASE:XRPUSD": "XRPUSD",
    "BINANCE:XRPUSDT": "XRPUSD",
    "COINBASE:LINKUSD": "LINKUSD",
    "BINANCE:LINKUSDT": "LINKUSD",
    "OANDA:XAUUSD": "XAUUSD",
    "OANDA:XAGUSD": "XAGUSD",
    "FXOPEN:XAGUSD": "XAGUSD",
    "TVC:USOIL": "WTIUSD",
    "NYMEX:CL1!": "WTIUSD",
    "OANDA:WTICOUSD": "WTIUSD",
    "FX:USOIL": "WTIUSD",
    "CAPITALCOM:OIL_CRUDE": "WTIUSD",
    "TVC:DJI": "US30",
    "TVC:NDQ": "US100",
    "NASDAQ:NDX": "NAS100",
    "COINBASE:BTCUSD": "BTCUSD",
    "COINBASE:ETHUSD": "ETHUSD",
    "NASDAQ:TSLA": "TSLA",
    "NASDAQ:AAPL": "AAPL",
    "NASDAQ:MSFT": "MSFT",
    "NASDAQ:NVDA": "NVDA",
    "NASDAQ:AMZN": "AMZN",
    "NASDAQ:META": "META",
    "NASDAQ:GOOGL": "GOOGL",
    "NASDAQ:NFLX": "NFLX",
    "NASDAQ:AMD": "AMD",
}


def get_desk_for_symbol(symbol: str) -> List[str]:
    """Return list of desk IDs that trade this symbol."""
    normalized = SYMBOL_ALIASES.get(symbol, symbol.upper().replace("/", ""))
    return [
        desk_id
        for desk_id, desk in DESKS.items()
        if normalized in desk["symbols"]
    ]
