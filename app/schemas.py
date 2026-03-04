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
        """Normalize alert type to lowercase snake_case."""
        return v.strip().lower().replace(" ", "_").replace("-", "_")

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
