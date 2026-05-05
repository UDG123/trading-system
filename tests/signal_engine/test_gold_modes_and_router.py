from app.services.signal_engine.gold_modes import scan_gold_modes
from app.services.signal_engine.regime_stack_router import route_stacks


def test_gold_modes_xauusd_only():
    assert scan_gold_modes("EURUSD", "TRENDING") == []
    out = scan_gold_modes("XAUUSD", "TRENDING", spread_ok=True, timeframe_state={"1M": True, "15M": True, "1H": True, "4H": True, "D": True})
    modes = {o["desk_mode"] for o in out}
    assert {"GOLD_SCALP", "GOLD_INTRADAY", "GOLD_SWING"}.issubset(modes)


def test_regime_stack_router_gold_scalp():
    stacks = route_stacks("DESK4_GOLD", "XAUUSD", "5M", "RANGING", mode="GOLD_SCALP")
    assert all(s in ["A", "C", "D"] for s in stacks)
