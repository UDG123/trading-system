"""Live data primitives for OniQuant signal generation.

This package is intentionally provider-agnostic. It provides normalized
market events, spread tracking, bar aggregation, and feeder scaffolding for
paper-trading signal generation. It does not place broker orders.
"""

from app.services.live_data.normalizer import BarEvent, QuoteEvent, TickEvent

__all__ = ["TickEvent", "QuoteEvent", "BarEvent"]
