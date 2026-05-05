import os
from pydantic_settings import BaseSettings, SettingsConfigDict


def _bool(name: str, default: bool = False) -> bool:
    return os.getenv(name, str(default)).strip().lower() in {"1", "true", "yes", "on"}


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', extra='ignore')

    app_name: str = 'OniQuant Lux Prop Engine'
    env: str = 'dev'
    port: int = 8000
    database_url: str = 'sqlite:///./oniquant.db'
    redis_url: str = 'redis://localhost:6379/0'

    signal_source: str = 'INTERNAL_ENGINE'
    enable_tradingview_webhook: bool = False
    live_data_enabled: bool = True
    data_provider: str = 'TWELVEDATA'
    twelvedata_api_key: str | None = None
    polygon_api_key: str | None = None
    worker_scanner_enabled: bool = True
    worker_pipeline_enabled: bool = True
    worker_execution_enabled: bool = True
    internal_scanner_interval_seconds: int = 60
    internal_scanner_max_symbols_per_cycle: int = 50
    internal_signal_dedup_minutes: int = 15
    webhook_secret: str | None = None

    live_trading_enabled: bool = False
    mcp_execution_enabled: bool = False
    broker_api_key: str | None = None

    account_equity: float = 100000.0
    max_risk_per_trade_pct: float = 0.5
    max_daily_loss_pct: float = 3.0
    max_open_trades: int = 3
    max_trades_per_day: int = 10
    min_rr: float = 1.5

    enable_crypto_desk: bool = False
    enable_fx_exotics: bool = False

    telegram_bot_token: str | None = None
    telegram_chat_id: str | None = None
    telegram_portfolio_chat: str | None = None
    telegram_system_chat: str | None = None


settings = Settings()

# Backward-compatible module constants used by legacy services.
DATABASE_URL = settings.database_url
REDIS_URL = settings.redis_url
SIGNAL_SOURCE = settings.signal_source
ENABLE_TRADINGVIEW_WEBHOOK = settings.enable_tradingview_webhook
LIVE_DATA_ENABLED = settings.live_data_enabled
DATA_PROVIDER = settings.data_provider
TWELVEDATA_API_KEY = settings.twelvedata_api_key or ""
POLYGON_API_KEY = settings.polygon_api_key or ""
WORKER_SCANNER_ENABLED = settings.worker_scanner_enabled
WORKER_PIPELINE_ENABLED = settings.worker_pipeline_enabled
WORKER_EXECUTION_ENABLED = settings.worker_execution_enabled
INTERNAL_SCANNER_INTERVAL_SECONDS = settings.internal_scanner_interval_seconds
INTERNAL_SCANNER_MAX_SYMBOLS_PER_CYCLE = settings.internal_scanner_max_symbols_per_cycle
INTERNAL_SIGNAL_DEDUP_MINUTES = settings.internal_signal_dedup_minutes
WEBHOOK_SECRET = settings.webhook_secret or ""
LIVE_TRADING_ENABLED = settings.live_trading_enabled
MCP_EXECUTION_ENABLED = settings.mcp_execution_enabled

TELEGRAM_BOT_TOKEN = settings.telegram_bot_token or ""
TELEGRAM_CHAT_ID = settings.telegram_chat_id or ""
TELEGRAM_PORTFOLIO_CHAT = settings.telegram_portfolio_chat or TELEGRAM_CHAT_ID
TELEGRAM_SYSTEM_CHAT = settings.telegram_system_chat or TELEGRAM_CHAT_ID
TELEGRAM_DESK_CHANNELS = {
    "DESK1_SCALPER": os.getenv("TELEGRAM_DESK1_CHAT", TELEGRAM_CHAT_ID),
    "DESK2_INTRADAY": os.getenv("TELEGRAM_DESK2_CHAT", TELEGRAM_CHAT_ID),
    "DESK3_SWING": os.getenv("TELEGRAM_DESK3_CHAT", TELEGRAM_CHAT_ID),
    "DESK4_GOLD": os.getenv("TELEGRAM_DESK4_CHAT", TELEGRAM_CHAT_ID),
    "DESK5_ALTS": os.getenv("TELEGRAM_DESK5_CHAT", TELEGRAM_CHAT_ID),
    "DESK6_EQUITIES": os.getenv("TELEGRAM_DESK6_CHAT", TELEGRAM_CHAT_ID),
}

CAPITAL_PER_ACCOUNT = settings.account_equity
PORTFOLIO_CAPITAL_PER_DESK = CAPITAL_PER_ACCOUNT / 6
MAX_DAILY_LOSS_PER_ACCOUNT = CAPITAL_PER_ACCOUNT * settings.max_daily_loss_pct / 100
MAX_TOTAL_LOSS_PER_ACCOUNT = CAPITAL_PER_ACCOUNT * 0.10
FIRM_WIDE_DAILY_DRAWDOWN_HALT = settings.max_daily_loss_pct
MAX_TOTAL_OPEN_POSITIONS = settings.max_open_trades
MAX_PORTFOLIO_RISK_PCT = settings.max_risk_per_trade_pct
DESK_DAILY_HARD_STOP_PCT = settings.max_daily_loss_pct
DEDUP_WINDOW_MINUTES = int(os.getenv("DEDUP_WINDOW_MINUTES", "15"))
MAX_CORRELATED_POSITIONS = int(os.getenv("MAX_CORRELATED_POSITIONS", "2"))

VALID_ALERT_TYPES = {
    "bullish_confirmation", "bearish_confirmation", "bullish_plus", "bearish_plus",
    "bullish_confirmation_plus", "bearish_confirmation_plus", "bullish_exit", "bearish_exit",
    "contrarian_bullish", "contrarian_bearish", "confirmation_turn_bullish",
    "confirmation_turn_bearish", "confirmation_turn_plus", "take_profit", "stop_loss",
    "smart_trail_cross", "smc_bullish_bos", "smc_bearish_bos", "smc_bullish_choch",
    "smc_bearish_choch", "smc_bullish_fvg", "smc_bearish_fvg", "smc_equal_highs",
    "smc_equal_lows", "smc_bullish_ob_break", "smc_bearish_ob_break",
}

DESKS = {
    "DESK1_SCALPER": {
        "name": "FX Scalper", "role": "FX_SCALP", "risk_pct": 0.25, "max_simultaneous": 2,
        "max_trades_day": 8, "symbols": ["EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCHF"],
        "timeframes": {"entry": "1M", "confirm": "5M,15M", "bias": "1H"},
    },
    "DESK2_INTRADAY": {
        "name": "FX Intraday", "role": "FX_INTRADAY", "risk_pct": 0.5, "max_simultaneous": 2,
        "max_trades_day": 6, "symbols": ["EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCHF"],
        "timeframes": {"entry": "15M", "confirm": "1H", "bias": "4H"},
    },
    "DESK3_SWING": {
        "name": "FX Swing", "role": "FX_SWING", "risk_pct": 0.75, "max_simultaneous": 2,
        "max_trades_day": 3, "symbols": ["EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCHF"],
        "timeframes": {"entry": "4H", "confirm": "D", "bias": "W"},
    },
    "DESK4_GOLD": {
        "name": "Gold", "role": "GOLD", "risk_pct": 0.5, "max_simultaneous": 2,
        "max_trades_day": 5, "symbols": ["XAUUSD"],
        "timeframes": {"entry": "1M", "confirm": "15M", "bias": "1H"},
    },
    "DESK5_ALTS": {
        "name": "Alts / Indices", "role": "ALTS", "risk_pct": 0.35, "max_simultaneous": 2,
        "max_trades_day": 4, "symbols": ["BTCUSD", "ETHUSD", "NAS100", "US30"],
        "timeframes": {"entry": "15M", "confirm": "1H", "bias": "4H"},
    },
    "DESK6_EQUITIES": {
        "name": "Equities", "role": "EQUITIES", "risk_pct": 0.25, "max_simultaneous": 2,
        "max_trades_day": 4, "symbols": ["AAPL", "MSFT", "NVDA", "TSLA"],
        "timeframes": {"entry": "15M", "confirm": "1H", "bias": "D"},
    },
}

SYMBOL_ALIASES = {
    "OANDA:EURUSD": "EURUSD", "OANDA:GBPUSD": "GBPUSD", "OANDA:USDJPY": "USDJPY",
    "OANDA:XAUUSD": "XAUUSD", "TVC:GOLD": "XAUUSD", "XAUUSD": "XAUUSD",
    "EUR/USD": "EURUSD", "GBP/USD": "GBPUSD", "USD/JPY": "USDJPY", "BTC/USD": "BTCUSD",
}

SESSION_WINDOWS = {"LONDON": (7, 16), "NEW_YORK": (13, 21), "ASIA": (22, 7)}
CORRELATION_GROUPS = {
    "USD_MAJORS": ["EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCHF"],
    "CRYPTO": ["BTCUSD", "ETHUSD"],
    "EQUITIES": ["AAPL", "MSFT", "NVDA", "TSLA", "NAS100", "US30"],
}
CONSECUTIVE_LOSS_RULES = {"pause_after": 3, "pause_minutes": 60}
SCORE_WEIGHTS = {
    "entry_trigger_normal": 1, "entry_trigger_plus": 2, "defined_sl_tp": 1,
    "confirmation_turn_plus": 2, "kill_zone_overlap": 2, "kill_zone_single": 1,
    "ml_confirm_per_tf": 1, "liquidity_sweep": 3, "conflicting_htf": -3,
    "adx_trending": 1, "adx_ranging": -1, "vix_elevated": -2, "vix_gold_bullish": 1,
    "dxy_headwind": -1, "multi_analyst_agree": 2,
}
SCORE_THRESHOLDS = {"HIGH": 7, "MEDIUM": 5, "LOW": 3, "min_total": 3, "high_quality": 7}
VIX_REGIMES = {"LOW": 15, "NORMAL": 25, "ELEVATED": 25, "HIGH": 35, "low": 15, "normal": 25, "high": 35}
MTF_SCORING = {"enabled": True}
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")

ENABLE_ADAPTIVE_INDICATORS = _bool("ENABLE_ADAPTIVE_INDICATORS", True)
ENABLE_MEAN_REVERSION = _bool("ENABLE_MEAN_REVERSION", True)
ENABLE_HMM_REGIME = _bool("ENABLE_HMM_REGIME", False)
ENABLE_MTF_SCORING = _bool("ENABLE_MTF_SCORING", True)
ENABLE_ICHIMOKU_FILTER = _bool("ENABLE_ICHIMOKU_FILTER", True)
ENABLE_ECON_CALENDAR = _bool("ENABLE_ECON_CALENDAR", False)
ENABLE_QUALITY_SCORER = _bool("ENABLE_QUALITY_SCORER", True)
ENABLE_MARKET_HOURS_FILTER = _bool("ENABLE_MARKET_HOURS_FILTER", True)
ENABLE_ORDER_FLOW = _bool("ENABLE_ORDER_FLOW", False)
ENABLE_DRAWDOWN_SCALING = _bool("ENABLE_DRAWDOWN_SCALING", True)
ENABLE_META_LABELER = _bool("ENABLE_META_LABELER", False)
ENABLE_VOL_TARGETING = _bool("ENABLE_VOL_TARGETING", False)
ENABLE_PARTIAL_EXITS = _bool("ENABLE_PARTIAL_EXITS", True)


def get_desk_for_symbol(symbol: str) -> list[str]:
    s = SYMBOL_ALIASES.get((symbol or "").upper(), (symbol or "").upper().replace("/", ""))
    return [desk_id for desk_id, desk in DESKS.items() if s in desk.get("symbols", [])]


def get_pip_info(symbol: str) -> dict:
    symbol = (symbol or "").upper()
    if symbol.endswith("JPY"):
        return {"pip_size": 0.01, "pip_value": 9.0}
    if symbol == "XAUUSD":
        return {"pip_size": 0.1, "pip_value": 1.0}
    if symbol in {"BTCUSD", "ETHUSD"} or symbol.startswith(("NAS", "US3")):
        return {"pip_size": 1.0, "pip_value": 1.0}
    return {"pip_size": 0.0001, "pip_value": 10.0}


def get_atr_settings(desk_id: str, symbol: str, timeframe: str) -> dict:
    if symbol == "XAUUSD":
        return {"sl_mult": 1.5, "tp1_mult": 3.0, "tp2_mult": 5.0}
    if desk_id == "DESK1_SCALPER":
        return {"sl_mult": 1.0, "tp1_mult": 2.0, "tp2_mult": 3.0}
    if desk_id == "DESK3_SWING":
        return {"sl_mult": 2.0, "tp1_mult": 4.0, "tp2_mult": 6.0}
    return {"sl_mult": 1.5, "tp1_mult": 3.0, "tp2_mult": 4.5}


def calculate_lot_size(equity: float, risk_pct: float, stop_pips: float, symbol: str) -> float:
    if stop_pips <= 0:
        return 0.01
    pip_value = get_pip_info(symbol)["pip_value"]
    risk_amount = equity * risk_pct / 100
    return round(max(0.01, risk_amount / (stop_pips * pip_value)), 2)


def get_hurst_thresholds(symbol: str) -> dict:
    return {"trend": 0.52, "chop": 0.45}


SIGNAL_DEBUG_MODE = _bool("SIGNAL_DEBUG_MODE", False)
QUALITY_SCORE_THRESHOLD = int(os.getenv("QUALITY_SCORE_THRESHOLD", "65"))
MIN_CONFLUENCE_SCORE = float(os.getenv("MIN_CONFLUENCE_SCORE", "6.5"))
MIN_TIMEFRAME_BARS = int(os.getenv("MIN_TIMEFRAME_BARS", "50"))
ALLOW_WEAK_TEST_SIGNALS = _bool("ALLOW_WEAK_TEST_SIGNALS", False)
SIMULATION_LATENCY_MS = int(os.getenv("SIMULATION_LATENCY_MS", "100"))
SIMULATION_SLIPPAGE_BPS = float(os.getenv("SIMULATION_SLIPPAGE_BPS", "1.0"))
SIMULATION_ASSUME_SL_FIRST_ON_SAME_BAR = _bool("SIMULATION_ASSUME_SL_FIRST_ON_SAME_BAR", True)
