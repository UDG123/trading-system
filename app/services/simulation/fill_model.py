from dataclasses import dataclass


@dataclass
class FillEstimate:
    side: str
    fill_price: float
    spread_bps: float
    slippage: float
    latency_ms: int


def estimate_fill(side: str, bid: float, ask: float, latency_ms: int = 100, slippage_bps: float = 1.0) -> FillEstimate:
    mid = (bid + ask) / 2 if bid > 0 and ask > 0 else 0
    spread = ask - bid if ask >= bid else 0
    spread_bps = (spread / mid * 10_000) if mid > 0 else 0
    px = ask if side.upper() == "BUY" else bid
    slip = px * (slippage_bps / 10_000) * (1 + (latency_ms / 1000) * 0.1)
    fill = px + slip if side.upper() == "BUY" else px - slip
    return FillEstimate(side=side.upper(), fill_price=fill, spread_bps=spread_bps, slippage=slip, latency_ms=latency_ms)


def resolve_same_bar_exit(direction: str, bar_low: float, bar_high: float, stop_loss: float, take_profit: float) -> str:
    """Conservative rule: if both touched in same bar, assume SL first."""
    touched_sl = bar_low <= stop_loss <= bar_high
    touched_tp = bar_low <= take_profit <= bar_high
    if touched_sl and touched_tp:
        return "SL"
    if touched_sl:
        return "SL"
    if touched_tp:
        return "TP"
    return "NONE"
