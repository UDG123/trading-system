from datetime import datetime, timezone, timedelta
from app.services.live_data.bar_aggregator import BarAggregator
from app.services.live_data.normalizer import TickEvent


def test_emit_completed_bar_on_bucket_roll():
    agg = BarAggregator()
    t0 = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    bars = agg.ingest_tick(TickEvent("XAUUSD", 2000, 1, t0, "test"))
    assert bars == []
    bars = agg.ingest_tick(TickEvent("XAUUSD", 2001, 1, t0 + timedelta(minutes=1), "test"))
    assert any(b.timeframe == "1M" for b in bars)
