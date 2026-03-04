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
# FIRM-WIDE RISK
# ─────────────────────────────────────────────────────────────
FIRM_CAPITAL = 600_000
ACCOUNTS = 6
CAPITAL_PER_ACCOUNT = 100_000
MAX_DAILY_LOSS_PER_ACCOUNT = 5_000
MAX_TOTAL_LOSS_PER_ACCOUNT = 10_000
FIRM_WIDE_DAILY_DRAWDOWN_HALT = 30_000  # reduce all desks 50%

# Consecutive loss protection thresholds
CONSECUTIVE_LOSS_RULES = {
    2: 0.75,   # 2 losses → 75% size
    3: 0.50,   # 3 losses → 50% size
    4: "PAUSE_2H",
    5: "CLOSE_DESK",
}

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
        "symbols": ["EURUSD", "USDJPY", "GBPUSD", "USDCHF", "XAGUSD"],
        "timeframes": {"bias": "15M", "confirmation": "5M", "entry": "1M"},
        "luxalgo_preset": "Scalper",
        "ml_classifier": 34,
        "sensitivity": 12,
        "trailing_stop_pips": 10,
        "max_trades_day": 15,
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
        "symbols": ["AUDUSD", "USDCAD", "NZDUSD", "EURUSD"],
        "timeframes": {"bias": "4H", "confirmation": "1H", "entry": "15M"},
        "luxalgo_preset": "Trend Trader",
        "ml_classifier": 500,
        "sensitivity": 19,
        "trailing_stop_pips": 30,
        "max_trades_day": 6,
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
        "symbols": ["EURJPY", "GBPJPY", "AUDJPY", "USDCAD"],
        "timeframes": {"bias": "W", "confirmation": "D", "entry": "4H"},
        "luxalgo_preset": "Swing Trader",
        "ml_classifier": 1234,
        "sensitivity": 25,
        "trailing_stop_pips": 80,
        "max_simultaneous": 3,
        "risk_pct": 2.0,
        "sessions": ["ALL"],
        "alerts": [
            "bullish_plus", "bearish_plus",
            "confirmation_turn_plus", "take_profit", "stop_loss",
        ],
    },
    "DESK4_GOLD": {
        "name": "Commodities",
        "style": "Gold Trend",
        "symbols": ["XAUUSD"],
        "timeframes": {"bias": "D", "confirmation": "4H,1H", "entry": "15M"},
        "luxalgo_preset": "Trend Trader + Neo Cloud",
        "ml_classifier": 34,
        "sensitivity": 19,
        "trailing_stop_pips": 20,
        "max_trades_day": 4,
        "risk_pct": 1.5,
        "sessions": ["LONDON_NY_OVERLAP"],
        "active_window": "08:00-16:00 ET",
        "alerts": [
            "bullish_confirmation", "bearish_confirmation",
            "bullish_plus", "bearish_plus",
            "contrarian_bullish", "contrarian_bearish",
            "take_profit", "stop_loss",
        ],
    },
    "DESK5_ALTS": {
        "name": "Alternative Assets",
        "style": "Index + Crypto Trend",
        "symbols": ["US30", "US100", "NAS100", "BTCUSD", "ETHUSD"],
        "timeframes": {"bias": "D", "confirmation": "4H", "entry": "1H"},
        "luxalgo_preset": "Trend Trader + Trend Strength",
        "ml_classifier": 500,
        "sensitivity": 19,
        "trailing_stop_pips": {"indices": 15, "crypto": 50},
        "max_trades_day": 8,
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
]

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
    "OANDA:XAUUSD": "XAUUSD",
    "OANDA:XAGUSD": "XAGUSD",
    "FXOPEN:XAGUSD": "XAGUSD",
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
