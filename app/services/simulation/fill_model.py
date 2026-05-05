from dataclasses import dataclass
from typing import Dict, Iterable

from app.config import (
    SIMULATION_ASSUME_SL_FIRST_ON_SAME_BAR,
    SIMULATION_LATENCY_MS,
    SIMULATION_SLIPPAGE_BPS,
)


@dataclass
class FillEstimate:
    side: str
    fill_price: float
    spread_bps: float
    slippage: float
    latency_ms: int


def estimate_entry_fill(side: str, bid: float, ask: float, latency_ms: int = SIMULATION_LATENCY_MS, slippage_bps: float = SIMULATION_SLIPPAGE_BPS) -> Dict:
    side_u = side.upper()
    base_px = ask if side_u == "BUY" else bid
    mid = (bid + ask) / 2 if bid > 0 and ask > 0 else base_px
    spread_bps = ((ask - bid) / mid * 10_000) if mid > 0 and ask >= bid else 0.0
    slip = base_px * (slippage_bps / 10_000)
    fill = base_px + slip if side_u == "BUY" else base_px - slip
    return {
        "side": side_u,
        "entry_fill_price": fill,
        "spread_bps": spread_bps,
        "slippage_bps": slippage_bps,
        "latency_ms": latency_ms,
    }


def evaluate_bar_exit(direction: str, bar_low: float, bar_high: float, stop_loss: float, take_profit: float, assume_sl_first_on_same_bar: bool = SIMULATION_ASSUME_SL_FIRST_ON_SAME_BAR) -> Dict:
    long = direction.upper() in {"LONG", "BUY"}
    sl_hit = bar_low <= stop_loss <= bar_high
    tp_hit = bar_low <= take_profit <= bar_high
    if sl_hit and tp_hit and assume_sl_first_on_same_bar:
        return {"exit_reason": "SL", "pessimistic_same_bar_applied": True}
    if tp_hit:
        return {"exit_reason": "TP", "pessimistic_same_bar_applied": False}
    if sl_hit:
        return {"exit_reason": "SL", "pessimistic_same_bar_applied": False}
    return {"exit_reason": "NONE", "pessimistic_same_bar_applied": False}


def simulate_trade_path(direction: str, entry_fill_price: float, stop_loss: float, take_profit: float, bars: Iterable[Dict]) -> Dict:
    exit_price = entry_fill_price
    exit_reason = "OPEN"
    pessimistic = False
    for b in bars:
        out = evaluate_bar_exit(direction, float(b["low"]), float(b["high"]), stop_loss, take_profit)
        if out["exit_reason"] != "NONE":
            exit_reason = out["exit_reason"]
            pessimistic = out["pessimistic_same_bar_applied"]
            exit_price = stop_loss if exit_reason == "SL" else take_profit
            break
    risk = abs(entry_fill_price - stop_loss) or 1e-9
    pnl = (exit_price - entry_fill_price) if direction.upper() in {"LONG", "BUY"} else (entry_fill_price - exit_price)
    return {
        "entry_fill_price": entry_fill_price,
        "exit_price": exit_price,
        "exit_reason": exit_reason,
        "pnl_r": pnl / risk,
        "spread_bps": 0.0,
        "slippage_bps": SIMULATION_SLIPPAGE_BPS,
        "pessimistic_same_bar_applied": pessimistic,
    }


# backwards compatibility

def estimate_fill(side: str, bid: float, ask: float, latency_ms: int = SIMULATION_LATENCY_MS, slippage_bps: float = SIMULATION_SLIPPAGE_BPS) -> FillEstimate:
    out = estimate_entry_fill(side, bid, ask, latency_ms, slippage_bps)
    return FillEstimate(side=out["side"], fill_price=out["entry_fill_price"], spread_bps=out["spread_bps"], slippage=out["entry_fill_price"] * (slippage_bps / 10_000), latency_ms=latency_ms)


def resolve_same_bar_exit(direction: str, bar_low: float, bar_high: float, stop_loss: float, take_profit: float) -> str:
    return evaluate_bar_exit(direction, bar_low, bar_high, stop_loss, take_profit)["exit_reason"]
