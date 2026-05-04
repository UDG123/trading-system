from typing import List, Optional

from app.services.signal_engine.strategy_stacks import select_stacks_for_regime


def route_stacks(
    desk_id: str,
    symbol: str,
    timeframe: str,
    regime: str,
    volatility_state: Optional[str] = None,
    mode: Optional[str] = None,
) -> List[str]:
    stacks = select_stacks_for_regime(regime or "TRANSITIONAL", symbol=symbol)
    if desk_id == "DESK4_GOLD":
        if mode == "GOLD_SCALP":
            return [s for s in stacks if s in ["A", "C", "D"]]
        if mode == "GOLD_INTRADAY":
            return [s for s in stacks if s in ["B", "C", "A"]]
        if mode == "GOLD_SWING":
            return [s for s in stacks if s in ["B", "E"]]
    if volatility_state == "VOLATILE" and "A" not in stacks:
        stacks.append("A")
    return stacks
