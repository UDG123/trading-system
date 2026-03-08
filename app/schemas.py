"""
Pydantic Schemas - Request/Response validation for webhook alerts.
"""
from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel, Field, field_validator


class TradingViewAlert(BaseModel):
    """
    Expected JSON payload from TradingView webhook.
    Configure your TradingView alert message as JSON matching this schema.

    Example TradingView alert message:
    {
        "secret": "your-webhook-secret",
        "symbol": "{{ticker}}",
        "exchange": "{{exchange}}",
        "timeframe": "{{interval}}",
        "alert_type": "bullish_confirmation",
        "price": {{close}},
        "time": "{{time}}",
        "tp1": {{plot("TP1")}},
        "tp2": {{plot("TP2")}},
        "sl1": {{plot("SL1")}},
        "sl2": {{plot("SL2")}},
        "smart_trail": {{plot("Smart Trail")}},
        "bar_index": {{bar_index}},
        "volume": {{volume}}
    }
    """
    # Authentication
    secret: str = Field(..., description="Webhook secret for authentication")

    # Core signal data
    symbol: str = Field(..., description="Ticker symbol from TradingView")
    exchange: Optional[str] = Field(None, description="Exchange identifier")
    timeframe: str = Field(..., description="Chart timeframe")
    alert_type: str = Field(..., description="LuxAlgo alert type")
    price: float = Field(..., gt=0, description="Current price at alert")
    time: Optional[str] = Field(None, description="Alert timestamp from TV")

    # LuxAlgo levels
    tp1: Optional[float] = Field(None, description="Take Profit 1")
    tp2: Optional[float] = Field(None, description="Take Profit 2")
    sl1: Optional[float] = Field(None, description="Stop Loss 1")
    sl2: Optional[float] = Field(None, description="Stop Loss 2")
    smart_trail: Optional[float] = Field(None, description="Smart Trail level")

    # Extra context
    bar_index: Optional[int] = Field(None, description="Bar index")
    volume: Optional[float] = Field(None, description="Volume at alert")

    @field_validator("alert_type")
    @classmethod
    def normalize_alert_type(cls, v: str) -> str:
        """Normalize alert type to lowercase snake_case.
        Handles both direct alert_type codes and LuxAlgo {default} messages.
        """
        # LuxAlgo {default} message → internal alert_type mapping
        luxalgo_message_map = {
            "bullish confirmation signal": "bullish_confirmation",
            "bearish confirmation signal": "bearish_confirmation",
            "bullish+ confirmation signal": "bullish_plus",
            "bearish+ confirmation signal": "bearish_plus",
            "strong bullish confirmation signal": "bullish_plus",
            "strong bearish confirmation signal": "bearish_plus",
            "bullish exit signal": "bullish_exit",
            "bearish exit signal": "bearish_exit",
            "bullish contrarian signal": "contrarian_bullish",
            "bearish contrarian signal": "contrarian_bearish",
            "confirmation turn bullish": "confirmation_turn_bullish",
            "confirmation turn bearish": "confirmation_turn_bearish",
            "confirmation turn plus": "confirmation_turn_plus",
            "take profit": "take_profit",
            "stop loss": "stop_loss",
            "smart trail cross": "smart_trail_cross",
            "smart trail crossed": "smart_trail_cross",
            # SMC structure alerts
            "internal bullish bos formed": "smc_bullish_bos",
            "bearish bos formed": "smc_bearish_bos",
            "internal bullish choch formed": "smc_bullish_choch",
            "bearish choch formed": "smc_bearish_choch",
            "bullish fvg formed": "smc_bullish_fvg",
            "bearish fvg formed": "smc_bearish_fvg",
            "equal highs detected": "smc_equal_highs",
            "equal lows detected": "smc_equal_lows",
            "price broke bullish internal ob": "smc_bullish_ob_break",
            "price broke bearish internal ob": "smc_bearish_ob_break",
            "price broke bullish swing ob": "smc_bullish_ob_break",
            "price broke bearish swing ob": "smc_bearish_ob_break",
        }

        cleaned = v.strip().lower()

        # Check if it's a LuxAlgo default message
        if cleaned in luxalgo_message_map:
            return luxalgo_message_map[cleaned]

        # Otherwise normalize to snake_case
        return cleaned.replace(" ", "_").replace("-", "_").replace("+", "_plus")

    @field_validator("symbol")
    @classmethod
    def clean_symbol(cls, v: str) -> str:
        return v.strip().upper()

    @field_validator("timeframe")
    @classmethod
    def clean_timeframe(cls, v: str) -> str:
        return v.strip().upper()


class SignalResponse(BaseModel):
    """Response returned after processing a webhook alert."""
    status: str  # "accepted" | "rejected" | "error"
    signal_id: Optional[int] = None
    symbol: Optional[str] = None
    alert_type: Optional[str] = None
    desks_matched: Optional[List[str]] = None
    is_valid: bool = False
    validation_errors: Optional[List[str]] = None
    message: str = ""


class HealthResponse(BaseModel):
    status: str
    database: str
    uptime_seconds: float
    signals_today: int
    version: str = "1.0.0"


class DeskSummary(BaseModel):
    desk_id: str
    name: str
    is_active: bool
    trades_today: int
    daily_pnl: float
    consecutive_losses: int
    size_modifier: float
    open_positions: int


class DashboardResponse(BaseModel):
    firm_status: str
    total_signals_today: int
    total_trades_today: int
    total_daily_pnl: float
    desks: List[DeskSummary]
