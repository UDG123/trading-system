"""
OniQuant v5.9 — Institutional Trading Floor Configuration
6 Desks · 8 Analysts · 1 CTO · $600K Under Management
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
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "OniQuant_X9k7mP2w_2026")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
TWELVEDATA_API_KEY = os.getenv("TWELVEDATA_API_KEY", "")
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "")
FMP_API_KEY = os.getenv("FMP_API_KEY", "")
BINANCE_WS_URL = "wss://stream.binance.com:9443/ws"
BINANCE_REST_URL = "https://api.binance.com"
BINANCE_WS_ENABLED = os.getenv("BINANCE_WS_ENABLED", "true").lower() in ("true", "1", "yes")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# Legacy MetaApi (deprecated — Alpaca Paper Trading is now primary)
METAAPI_TOKEN = os.getenv("METAAPI_TOKEN", "")
METAAPI_ACCOUNT_ID = os.getenv("METAAPI_ACCOUNT_ID", "")
METAAPI_REGION = os.getenv("METAAPI_REGION", "new-york")

# ─────────────────────────────────────────────────────────────
# TELEGRAM
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
TELEGRAM_SYSTEM_CHAT = os.getenv("TG_SYSTEM", "-1003710749613")

# ─────────────────────────────────────────────────────────────
# CAPITAL AND RISK
# ─────────────────────────────────────────────────────────────
ACCOUNTS = 6
CAPITAL_PER_ACCOUNT = 100_000
FIRM_CAPITAL = ACCOUNTS * CAPITAL_PER_ACCOUNT
PORTFOLIO_CAPITAL = 1_000_000

MAX_DAILY_LOSS_PCT = 5.0
MAX_TOTAL_LOSS_PCT = 10.0
MAX_DAILY_LOSS_PER_ACCOUNT = int(CAPITAL_PER_ACCOUNT * MAX_DAILY_LOSS_PCT / 100)
MAX_TOTAL_LOSS_PER_ACCOUNT = int(CAPITAL_PER_ACCOUNT * MAX_TOTAL_LOSS_PCT / 100)
FIRM_WIDE_DAILY_DRAWDOWN_HALT = 30_000

# Per-desk daily stop
DESK_DAILY_SOFT_STOP_PCT = 3.0
DESK_DAILY_HARD_STOP_PCT = 4.0

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
    "XAUUSD": "GOLD", "XAGUSD": "SILVER",
    "WTIUSD": "WTI", "XCUUSD": "COMMODITY",
    "US30": "INDEX", "US100": "INDEX", "NAS100": "INDEX",
    "BTCUSD": "CRYPTO", "ETHUSD": "CRYPTO",
    "SOLUSD": "CRYPTO", "XRPUSD": "CRYPTO", "LINKUSD": "CRYPTO",
    "TSLA": "EQUITY", "AAPL": "EQUITY", "MSFT": "EQUITY",
    "NVDA": "EQUITY", "AMZN": "EQUITY", "META": "EQUITY",
    "GOOGL": "EQUITY", "NFLX": "EQUITY", "AMD": "EQUITY",
}


def get_pip_info(symbol: str):
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
    if profile == "SRV_30":
        return 30
    return 100


# ═══════════════════════════════════════════════════════════════
# ATR SETTINGS PER DESK
# ═══════════════════════════════════════════════════════════════
ATR_SETTINGS = {
    "DESK1_SCALPER":     {"atr_period": 10, "sl_mult": 1.5, "tp1_mult": 3.0, "tp2_mult": 4.5, "min_rr": 1.5},
    "DESK2_INTRADAY":    {"atr_period": 14, "sl_mult": 2.0, "tp1_mult": 4.0, "tp2_mult": 6.0, "min_rr": 1.5},
    "DESK3_SWING":       {"atr_period": 14, "sl_mult": 2.0, "tp1_mult": 4.0, "tp2_mult": 6.0, "min_rr": 2.0},
    "DESK4_GOLD":        {"atr_period": 14, "sl_mult": 2.0, "tp1_mult": 4.0, "tp2_mult": 6.0, "min_rr": 1.5},
    "DESK4_GOLD_SCALP":  {"atr_period": 10, "sl_mult": 2.0, "tp1_mult": 4.0, "tp2_mult": 6.0, "min_rr": 1.5},
    "DESK4_GOLD_INTRA":  {"atr_period": 14, "sl_mult": 2.5, "tp1_mult": 5.0, "tp2_mult": 7.5, "min_rr": 1.5},
    "DESK4_GOLD_SWING":  {"atr_period": 14, "sl_mult": 2.0, "tp1_mult": 4.0, "tp2_mult": 6.0, "min_rr": 2.0},
    "DESK5_ALTS":        {"atr_period": 14, "sl_mult": 2.0, "tp1_mult": 4.0, "tp2_mult": 6.0, "min_rr": 1.5},
    "DESK5_ALTS_CRYPTO": {"atr_period": 10, "sl_mult": 2.5, "tp1_mult": 5.0, "tp2_mult": 7.5, "min_rr": 1.5},
    "DESK6_EQUITIES":    {"atr_period": 14, "sl_mult": 2.0, "tp1_mult": 4.0, "tp2_mult": 6.0, "min_rr": 1.5},
}


def get_atr_settings(desk_id: str, symbol: str = "", timeframe: str = "") -> Dict:
    sym = symbol.upper()
    if desk_id == "DESK4_GOLD":
        tf = timeframe.upper() if timeframe else ""
        if "5" in tf or "1M" == tf:
            return ATR_SETTINGS["DESK4_GOLD_SCALP"]
        elif "15" in tf or tf == "1H" or tf == "60":
            return ATR_SETTINGS["DESK4_GOLD_INTRA"]
        elif "4" in tf or "D" in tf or "W" in tf:
            return ATR_SETTINGS["DESK4_GOLD_SWING"]
        return ATR_SETTINGS["DESK4_GOLD"]
    if desk_id == "DESK5_ALTS" and any(c in sym for c in ["BTC", "ETH", "SOL", "XRP", "LINK"]):
        return ATR_SETTINGS["DESK5_ALTS_CRYPTO"]
    return ATR_SETTINGS.get(desk_id, ATR_SETTINGS["DESK2_INTRADAY"])


# ═══════════════════════════════════════════════════════════════
# VIX REGIME THRESHOLDS
# ═══════════════════════════════════════════════════════════════
VIX_REGIMES = {
    "NORMAL_MAX": 20,
    "ELEVATED": 25,
    "HIGH": 30,
    "EXTREME": 35,
}

# ═══════════════════════════════════════════════════════════════
# PROFILES
# ═══════════════════════════════════════════════════════════════
SRV_100 = {
    "name": "Server 1:100", "tag": "🟦", "leverage": 100, "capital_per_desk": 100_000,
    "DESK1_SCALPER":  {"risk_pct": 0.50, "lot_method": "DYNAMIC", "max_trades_day": 20, "max_open": 5, "max_daily_loss_pct": 4.0, "consecutive_loss_rules": {2: 0.75, 3: 0.50, 4: "PAUSE_1H", 5: "CLOSE_DESK"}, "trailing_stop_pips": 10},
    "DESK2_INTRADAY": {"risk_pct": 0.75, "lot_method": "DYNAMIC", "max_lot": 10.0, "max_trades_day": 10, "max_open": 5, "max_daily_loss_pct": 4.0, "consecutive_loss_rules": {2: 0.75, 3: 0.50, 4: "PAUSE_2H", 5: "CLOSE_DESK"}, "trailing_stop_pips": 30},
    "DESK3_SWING":    {"risk_pct": 1.0,  "lot_method": "DYNAMIC", "max_lot": 10.0, "max_trades_day": 6, "max_open": 5, "max_daily_loss_pct": 4.0, "consecutive_loss_rules": {2: 0.75, 3: 0.50, 4: "PAUSE_2H", 5: "CLOSE_DESK"}, "trailing_stop_pips": 80},
    "DESK4_GOLD":     {"sub_strategies": {"gold_scalp": {"risk_pct": 0.40, "max_trades": 8}, "gold_intra": {"risk_pct": 0.60, "max_trades": 6}, "gold_swing": {"risk_pct": 0.80, "max_trades": 3}}, "lot_method": "DYNAMIC", "max_lot": 5.0, "max_open": 6, "max_daily_loss_pct": 4.0, "consecutive_loss_rules": {2: 0.75, 3: 0.50, 4: "PAUSE_2H", 5: "CLOSE_DESK"}},
    "DESK5_ALTS":     {"risk_pct": 0.75, "risk_pct_crypto": 0.50, "lot_method": "DYNAMIC", "max_lot": 10.0, "max_trades_day": 10, "max_open": 5, "max_daily_loss_pct": 4.0, "consecutive_loss_rules": {2: 0.75, 3: 0.50, 4: "PAUSE_2H", 5: "CLOSE_DESK"}, "trailing_stop_pips": {"indices": 15, "crypto": 50}},
    "DESK6_EQUITIES": {"risk_pct": 0.75, "lot_method": "DYNAMIC", "max_lot": 10.0, "max_trades_day": 6, "max_open": 4, "max_daily_loss_pct": 4.0, "consecutive_loss_rules": {2: 0.75, 3: 0.50, 4: "PAUSE_2H", 5: "CLOSE_DESK"}, "trailing_stop_pips": 25},
}

SRV_30 = {
    "name": "Server 1:30", "tag": "🟧", "leverage": 30, "capital_per_desk": 100_000,
    "DESK1_SCALPER":  {"risk_pct": 0.40, "lot_method": "DYNAMIC", "max_trades_day": 8, "max_open": 2, "max_daily_loss_pct": 4.0, "consecutive_loss_rules": {2: 0.75, 3: 0.50, 4: "PAUSE_2H", 5: "CLOSE_DESK"}, "trailing_stop_pips": 10},
    "DESK2_INTRADAY": {"risk_pct": 0.60, "lot_method": "DYNAMIC", "max_lot": 3.0, "max_trades_day": 5, "max_open": 3, "max_daily_loss_pct": 4.0, "consecutive_loss_rules": {2: 0.75, 3: 0.50, 4: "PAUSE_2H", 5: "CLOSE_DESK"}, "trailing_stop_pips": 30},
    "DESK3_SWING":    {"risk_pct": 0.75, "lot_method": "DYNAMIC", "max_lot": 3.0, "max_trades_day": 3, "max_open": 3, "max_daily_loss_pct": 4.0, "consecutive_loss_rules": {2: 0.75, 3: 0.50, 4: "PAUSE_2H", 5: "CLOSE_DESK"}, "trailing_stop_pips": 80},
    "DESK4_GOLD":     {"sub_strategies": {"gold_scalp": {"risk_pct": 0.30, "max_trades": 4}, "gold_intra": {"risk_pct": 0.50, "max_trades": 3}, "gold_swing": {"risk_pct": 0.60, "max_trades": 1}}, "lot_method": "DYNAMIC", "max_lot": 2.0, "max_open": 2, "max_daily_loss_pct": 4.0, "consecutive_loss_rules": {2: 0.75, 3: 0.50, 4: "PAUSE_2H", 5: "CLOSE_DESK"}},
    "DESK5_ALTS":     {"risk_pct": 0.50, "risk_pct_crypto": 0.40, "lot_method": "DYNAMIC", "max_lot": 3.0, "max_trades_day": 4, "max_open": 2, "max_daily_loss_pct": 4.0, "consecutive_loss_rules": {2: 0.75, 3: 0.50, 4: "PAUSE_2H", 5: "CLOSE_DESK"}, "trailing_stop_pips": {"indices": 15, "crypto": 50}},
    "DESK6_EQUITIES": {"risk_pct": 0.50, "lot_method": "DYNAMIC", "max_lot": 3.0, "max_trades_day": 3, "max_open": 2, "max_daily_loss_pct": 4.0, "consecutive_loss_rules": {2: 0.75, 3: 0.50, 4: "PAUSE_2H", 5: "CLOSE_DESK"}, "trailing_stop_pips": 25},
}

MT5_1M = {
    "name": "MT5 $1M", "tag": "📟", "leverage": 100, "capital": 1_000_000,
    "max_daily_loss": 50_000, "max_total_loss": 100_000,
    "capital_per_desk": {
        "DESK1_SCALPER": 120_000, "DESK2_INTRADAY": 200_000, "DESK3_SWING": 200_000,
        "DESK4_GOLD": 180_000, "DESK5_ALTS": 150_000, "DESK6_EQUITIES": 150_000,
    },
    "DESK1_SCALPER":  {"risk_pct": 0.60, "lot_method": "DYNAMIC", "max_trades_day": 12, "max_open": 4},
    "DESK2_INTRADAY": {"risk_pct": 1.0,  "lot_method": "DYNAMIC", "max_trades_day": 8,  "max_open": 4},
    "DESK3_SWING":    {"risk_pct": 1.25, "lot_method": "DYNAMIC", "max_trades_day": 4,  "max_open": 4},
    "DESK4_GOLD":     {"sub_strategies": {"gold_scalp": {"risk_pct": 0.50, "max_trades": 8}, "gold_intra": {"risk_pct": 0.80, "max_trades": 5}, "gold_swing": {"risk_pct": 1.0, "max_trades": 3}}, "lot_method": "DYNAMIC", "max_open": 5},
    "DESK5_ALTS":     {"risk_pct": 1.0, "risk_pct_crypto": 0.60, "lot_method": "DYNAMIC", "max_trades_day": 8, "max_open": 4},
    "DESK6_EQUITIES": {"risk_pct": 1.0, "lot_method": "DYNAMIC", "max_trades_day": 6, "max_open": 4},
}

PORTFOLIO_CAPITAL_PER_DESK = MT5_1M.get("capital_per_desk", {})


def calculate_lot_size(desk_id: str, symbol: str, risk_pct: float, sl_pips: float, account_capital: float = None, profile: str = "SRV_100") -> float:
    if account_capital is None:
        account_capital = CAPITAL_PER_ACCOUNT
    pip_size, pip_value = get_pip_info(symbol)
    prof = SRV_100 if profile == "SRV_100" else (SRV_30 if profile == "SRV_30" else MT5_1M)
    desk_profile = prof.get(desk_id, {})
    if desk_profile.get("lot_method") == "FIXED" and desk_profile.get("fixed_lot"):
        return desk_profile["fixed_lot"]
    if sl_pips <= 0 or pip_value <= 0:
        return 0.01
    risk_dollars = account_capital * (risk_pct / 100)
    lot_size = risk_dollars / (sl_pips * pip_value)
    max_lot = desk_profile.get("max_lot", 10.0)
    lot_size = min(lot_size, max_lot)
    lot_size = max(lot_size, 0.01)
    return round(lot_size, 2)


CONSECUTIVE_LOSS_RULES = {2: 0.75, 3: 0.50, 4: "PAUSE_1H", 5: "CLOSE_DESK"}


# ═══════════════════════════════════════════════════════════════
# CONSENSUS SCORING v5.9
# ═══════════════════════════════════════════════════════════════
SCORE_THRESHOLDS = {
    "HIGH": 8,    # 100% size
    "MEDIUM": 5,  # 60% size
    "LOW": 3,     # 30% size
}

SCORE_WEIGHTS = {
    "entry_trigger_normal": 1,
    "entry_trigger_plus": 2,
    "confirmation_turn_plus": 2,
    "defined_sl_tp": 1,
    "bias_match": 3,
    "setup_match": 2,
    "conflicting_htf": -4,
    "kill_zone_overlap": 3,
    "kill_zone_single": 1,
    "ml_confirm_per_tf": 1,
    "correlation_confirm": 2,
    "liquidity_sweep": 3,
    "rsi_aligned": 2,
    "rsi_counter": -2,
    "adx_trending": 1,
    "adx_ranging": -1,
    "vix_elevated": -2,
    "vix_gold_bullish": 1,
    "dxy_headwind": -1,
    "multi_analyst_agree": 2,
    # Legacy keys (backwards compat)
    "bullish_bearish_plus": 2,
}


# ═══════════════════════════════════════════════════════════════
# DESK DEFINITIONS — FULL COVERAGE (40 symbols · 54 alerts)
# All desks: ML Classifier 34 + Overlay Filter + Once Per Bar Close
# OniAI virtual trades broadcast to Telegram for any CTO-approved
# signal that can't execute due to max_open limits.
# ═══════════════════════════════════════════════════════════════
DESKS: Dict[str, dict] = {
    "DESK1_SCALPER": {
        "name": "FX Scalper",
        "style": "Scalper",
        "symbols": ["EURUSD", "USDJPY", "GBPUSD", "USDCHF", "AUDUSD", "NZDUSD", "EURCHF"],
        "timeframes": {"bias": "15M", "confirmation": "5M", "entry": "1M"},
        "luxalgo_preset": "Scalper",
        "luxalgo_filter": "Smart Trail",
        "ml_classifier": 34,
        "sensitivity": 7,
        "trailing_stop_pips": 10,
        "max_trades_day": 20,
        "max_simultaneous": 5,
        "max_hold_hours": 1.5,
        "risk_pct": 0.5,
        "sessions": ["LONDON_OPEN", "NY_OPEN"],
        "alerts": [
            "bullish_confirmation", "bearish_confirmation",
            "bullish_confirmation_plus", "bearish_confirmation_plus",
            "bullish_exit", "bearish_exit", "take_profit", "stop_loss",
        ],
    },
    "DESK2_INTRADAY": {
        "name": "FX Intraday",
        "style": "Intraday",
        "symbols": [
            "EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCAD",
            "NZDUSD", "EURGBP", "GBPCAD", "AUDCAD", "CHFJPY", "EURNZD",
        ],
        "timeframes": {"bias": "4H", "confirmation": "1H", "entry": "15M"},
        "luxalgo_preset": "Trend Trader",
        "luxalgo_filter": "Trend Catcher",
        "ml_classifier": 34,
        "sensitivity": 14,
        "trailing_stop_pips": 30,
        "max_trades_day": 10,
        "max_simultaneous": 5,
        "max_hold_hours": 8,
        "risk_pct": 0.75,
        "sessions": ["LONDON", "NEW_YORK"],
        "alerts": [
            "bullish_confirmation", "bearish_confirmation",
            "bullish_plus", "bearish_plus",
            "bullish_confirmation_plus", "bearish_confirmation_plus",
            "bullish_exit", "bearish_exit",
            "take_profit", "stop_loss", "confirmation_turn_plus",
        ],
    },
    "DESK3_SWING": {
        "name": "Swing Trader",
        "style": "Swing",
        "symbols": [
            "EURUSD", "GBPUSD", "AUDUSD", "USDCAD", "NZDUSD",
            "EURJPY", "GBPJPY", "AUDJPY", "EURAUD", "GBPAUD",
            "CADJPY", "NZDJPY", "GBPNZD",
        ],
        "timeframes": {"bias": "W", "confirmation": "D", "entry": "4H"},
        "luxalgo_preset": "Swing Trader",
        "luxalgo_filter": "Trend Catcher",
        "ml_classifier": 34,
        "sensitivity": 14,
        "trailing_stop_pips": 80,
        "max_simultaneous": 5,
        "max_trades_day": 6,
        "max_hold_hours": 120,
        "risk_pct": 1.0,
        "sessions": ["ALL"],
        "close_friday_utc": 20,
        "alerts": [
            "bullish_confirmation", "bearish_confirmation",
            "bullish_plus", "bearish_plus",
            "bullish_confirmation_plus", "bearish_confirmation_plus",
            "confirmation_turn_plus",
            "bullish_exit", "bearish_exit", "take_profit", "stop_loss",
        ],
    },
    "DESK4_GOLD": {
        "name": "Gold Specialist",
        "style": "Multi-TF Gold + Commodities",
        "symbols": ["XAUUSD", "XAGUSD", "WTIUSD"],
        "timeframes": {"bias": "D", "confirmation": "4H,1H", "entry": "5M,15M,1H"},
        "luxalgo_preset": "Trend Trader + Neo Cloud",
        "luxalgo_filter": "Smart Trail",
        "ml_classifier": 34,
        "sensitivity": 12,
        "trailing_stop_pips": 30,
        "max_trades_day": 15,
        "max_simultaneous": 6,
        "max_hold_hours": 120,
        "risk_pct": 0.6,
        "sessions": ["LONDON", "NEW_YORK", "LONDON_NY_OVERLAP"],
        "active_window": "07:00-21:00 UTC",
        "dxy_correlation_threshold": -0.70,
        "vix_bullish_bias_above": 25,
        "alerts": [
            "bullish_confirmation", "bearish_confirmation",
            "bullish_plus", "bearish_plus",
            "bullish_confirmation_plus", "bearish_confirmation_plus",
            "bullish_exit", "bearish_exit", "take_profit", "stop_loss",
        ],
        "sub_strategies": {
            "gold_scalp": {
                "timeframe": "5M", "sensitivity": 7, "filter": "Smart Trail",
                "risk_pct": 0.4, "max_trades": 8, "max_hold_hours": 1.5,
                "trailing_stop_pips": 15, "sessions": ["LONDON_NY_OVERLAP"],
                "symbols": ["XAUUSD"],
                "alerts": ["bullish_confirmation", "bearish_confirmation", "bullish_confirmation_plus", "bearish_confirmation_plus"],
            },
            "gold_intra": {
                "timeframe": "15M,1H", "sensitivity": 12, "filter": "Smart Trail",
                "risk_pct": 0.6, "max_trades": 6, "max_hold_hours": 6,
                "trailing_stop_pips": 30, "sessions": ["LONDON", "NEW_YORK", "LONDON_NY_OVERLAP"],
                "symbols": ["XAUUSD"],
                "alerts": ["bullish_confirmation", "bearish_confirmation", "bullish_plus", "bearish_plus", "bullish_confirmation_plus", "bearish_confirmation_plus"],
            },
            "gold_swing": {
                "timeframe": "4H", "sensitivity": 14, "filter": "Trend Catcher",
                "risk_pct": 0.8, "max_trades": 3, "max_hold_hours": 120,
                "trailing_stop_pips": 50, "sessions": ["LONDON", "NEW_YORK"],
                "symbols": ["XAUUSD", "XAGUSD", "WTIUSD"],
                "alerts": ["bullish_confirmation", "bearish_confirmation", "bullish_plus", "bearish_plus", "bullish_confirmation_plus", "bearish_confirmation_plus"],
            },
        },
    },
    "DESK5_ALTS": {
        "name": "Momentum",
        "style": "Index + Crypto + Commodity Trend",
        "symbols": ["NAS100", "US30", "BTCUSD", "ETHUSD", "SOLUSD", "XRPUSD", "LINKUSD", "WTIUSD"],
        "timeframes": {"bias": "D", "confirmation": "4H", "entry": "1H"},
        "luxalgo_preset": "Trend Trader + Trend Strength",
        "luxalgo_filter": "Smart Trail",
        "ml_classifier": 34,
        "sensitivity": 14,
        "sensitivity_crypto": 16,
        "trailing_stop_pips": {"indices": 15, "crypto": 50, "commodity": 30},
        "max_trades_day": 10,
        "max_simultaneous": 5,
        "max_hold_hours": 24,
        "max_hold_hours_indices": 8,
        "risk_pct": 0.75,
        "risk_pct_crypto": 0.50,
        "sessions": ["US_MARKET", "WEEKDAY_CRYPTO"],
        "no_weekend_crypto": True,
        "vix_halt_above": 30,
        "alerts": [
            "bullish_confirmation", "bearish_confirmation",
            "bullish_plus", "bearish_plus",
            "bullish_confirmation_plus", "bearish_confirmation_plus",
            "bullish_exit", "bearish_exit", "take_profit", "stop_loss",
        ],
    },
    "DESK6_EQUITIES": {
        "name": "Equities",
        "style": "Stock Trend",
        "symbols": ["NVDA", "AAPL", "TSLA", "MSFT", "AMZN", "META", "GOOGL", "NFLX", "AMD"],
        "timeframes": {"bias": "D", "confirmation": "4H", "entry": "1H"},
        "luxalgo_preset": "Trend Trader",
        "luxalgo_filter": "Smart Trail",
        "ml_classifier": 34,
        "sensitivity": 12,
        "trailing_stop_pips": 25,
        "max_trades_day": 6,
        "max_simultaneous": 4,
        "max_hold_hours": 7,
        "risk_pct": 0.75,
        "sessions": ["US_MARKET"],
        "active_window": "13:30-20:00 UTC",
        "vix_halt_above": 30,
        "require_vwap_alignment": True,
        "alerts": [
            "bullish_confirmation", "bearish_confirmation",
            "bullish_plus", "bearish_plus",
            "bullish_confirmation_plus", "bearish_confirmation_plus",
            "contrarian_bullish", "contrarian_bearish",
            "bullish_exit", "bearish_exit", "take_profit", "stop_loss",
        ],
    },
}


VALID_ALERT_TYPES = [
    "bullish_confirmation", "bearish_confirmation",
    "bullish_plus", "bearish_plus",
    "bullish_confirmation_plus", "bearish_confirmation_plus",
    "bullish_exit", "bearish_exit",
    "confirmation_turn_bullish", "confirmation_turn_bearish", "confirmation_turn_plus",
    "contrarian_bullish", "contrarian_bearish",
    "take_profit", "stop_loss", "smart_trail_cross",
    "smc_structure", "smc_bullish_bos", "smc_bearish_bos",
    "smc_bullish_choch", "smc_bearish_choch",
    "smc_bullish_fvg", "smc_bearish_fvg",
    "smc_equal_highs", "smc_equal_lows",
    "smc_bullish_ob_break", "smc_bearish_ob_break",
]


SESSION_WINDOWS = {
    "LONDON":            {"start": 7,  "end": 16},
    "LONDON_OPEN":       {"start": 7,  "end": 10},
    "NEW_YORK":          {"start": 12, "end": 21},
    "NY_OPEN":           {"start": 12, "end": 15},
    "LONDON_NY_OVERLAP": {"start": 12, "end": 16},
    "US_MARKET":         {"start": 13, "end": 20},
    "MARKET_HOURS_INDICES": {"start": 13, "end": 20},
    "WEEKDAY_CRYPTO":    {"start": 13, "end": 21},
    "24_7_CRYPTO":       {"start": 0,  "end": 24},
    "SYDNEY":            {"start": 21, "end": 6},
    "TOKYO":             {"start": 23, "end": 8},
    "ALL":               {"start": 0,  "end": 24},
}


CORRELATION_GROUPS = {
    "USD_WEAKNESS":    {"symbols": ["EURUSD", "GBPUSD", "AUDUSD", "NZDUSD"], "long_correlated": True},
    "USD_STRENGTH":    {"symbols": ["USDJPY", "USDCHF", "USDCAD"], "long_correlated": True},
    "JPY_CROSSES":     {"symbols": ["EURJPY", "GBPJPY", "AUDJPY", "CADJPY", "NZDJPY", "CHFJPY"], "long_correlated": True},
    "EUR_CROSSES":     {"symbols": ["EURAUD", "EURGBP", "EURCHF", "EURNZD"], "long_correlated": True},
    "GBP_CROSSES":     {"symbols": ["GBPAUD", "GBPCAD", "GBPNZD"], "long_correlated": True},
    "AUD_CROSSES":     {"symbols": ["AUDCAD", "AUDUSD", "NZDUSD"], "long_correlated": True},
    "TECH_STOCKS":     {"symbols": ["NVDA", "AAPL", "TSLA", "MSFT", "AMZN", "META", "GOOGL", "NFLX", "AMD"], "long_correlated": True},
    "INDICES_CRYPTO":  {"symbols": ["NAS100", "US30", "BTCUSD"], "long_correlated": True},
    "CRYPTO":          {"symbols": ["BTCUSD", "ETHUSD", "SOLUSD", "XRPUSD", "LINKUSD"], "long_correlated": True},
    "PRECIOUS_METALS": {"symbols": ["XAUUSD", "XAGUSD"], "long_correlated": True},
    "ENERGY":          {"symbols": ["WTIUSD"], "long_correlated": True},
}

MAX_CORRELATED_POSITIONS = 2

SYMBOL_ALIASES = {
    "FX:EURUSD": "EURUSD", "FX:USDJPY": "USDJPY", "FX:GBPUSD": "GBPUSD",
    "FX:USDCHF": "USDCHF", "FX:AUDUSD": "AUDUSD", "FX:USDCAD": "USDCAD",
    "FX:NZDUSD": "NZDUSD", "FX:EURJPY": "EURJPY", "FX:GBPJPY": "GBPJPY",
    "FX:AUDJPY": "AUDJPY", "FX:EURGBP": "EURGBP", "FX:EURAUD": "EURAUD",
    "FX:GBPAUD": "GBPAUD", "FX:EURCHF": "EURCHF", "FX:CADJPY": "CADJPY",
    "FX:NZDJPY": "NZDJPY", "FX:GBPCAD": "GBPCAD", "FX:AUDCAD": "AUDCAD",
    "FX:AUDNZD": "AUDNZD", "FX:CHFJPY": "CHFJPY", "FX:EURNZD": "EURNZD",
    "FX:GBPNZD": "GBPNZD",
    "COMEX:HG1!": "XCUUSD", "TVC:COPPER": "XCUUSD", "OANDA:XCUUSD": "XCUUSD", "FX:COPPER": "XCUUSD",
    "COINBASE:SOLUSD": "SOLUSD", "BINANCE:SOLUSDT": "SOLUSD",
    "COINBASE:XRPUSD": "XRPUSD", "BINANCE:XRPUSDT": "XRPUSD",
    "COINBASE:LINKUSD": "LINKUSD", "BINANCE:LINKUSDT": "LINKUSD",
    "OANDA:XAUUSD": "XAUUSD", "OANDA:XAGUSD": "XAGUSD", "FXOPEN:XAGUSD": "XAGUSD",
    "TVC:USOIL": "WTIUSD", "NYMEX:CL1!": "WTIUSD", "OANDA:WTICOUSD": "WTIUSD",
    "FX:USOIL": "WTIUSD", "CAPITALCOM:OIL_CRUDE": "WTIUSD",
    "TVC:DJI": "US30", "TVC:NDQ": "US100", "NASDAQ:NDX": "NAS100",
    "OANDA:NAS100USD": "NAS100", "PEPPERSTONE:NAS100": "NAS100",
    "COINBASE:BTCUSD": "BTCUSD", "COINBASE:ETHUSD": "ETHUSD",
    "NASDAQ:TSLA": "TSLA", "NASDAQ:AAPL": "AAPL", "NASDAQ:MSFT": "MSFT",
    "NASDAQ:NVDA": "NVDA", "NASDAQ:AMZN": "AMZN", "NASDAQ:META": "META",
    "NASDAQ:GOOGL": "GOOGL", "NASDAQ:NFLX": "NFLX", "NASDAQ:AMD": "AMD",
}


def get_desk_for_symbol(symbol: str) -> List[str]:
    normalized = SYMBOL_ALIASES.get(symbol, symbol.upper().replace("/", ""))
    return [desk_id for desk_id, desk in DESKS.items() if normalized in desk["symbols"]]
