from datetime import datetime
from typing import Any, Literal
from pydantic import BaseModel, Field


class SignalEvent(BaseModel):
    event_id: str
    source: str
    symbol: str
    desk: str | None = None
    timeframe: str
    side: Literal['buy', 'sell']
    entry: float
    stop_loss: float | None = None
    take_profit: float
    confidence: float = 0.0
    strategy_name: str = 'unknown'
    raw_payload: dict[str, Any] = Field(default_factory=dict)
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class ValidatedSignal(BaseModel):
    original_event: SignalEvent
    validation_score: float
    approved: bool
    rejection_reasons: list[str] = Field(default_factory=list)
    risk_snapshot: dict[str, Any] = Field(default_factory=dict)


class OrderEvent(BaseModel):
    order_id: str
    signal_id: str
    symbol: str
    side: Literal['buy', 'sell']
    qty: float
    entry: float
    stop_loss: float
    take_profit: float
    status: str
    mode: Literal['paper', 'live'] = 'paper'
    timestamp: datetime = Field(default_factory=datetime.utcnow)
