from app.services.signal_engine.cross_desk_bias import CrossDeskBiasEngine
from app.services.signal_engine.gold_modes import scan_gold_modes


def test_signal_flow_smoke_by_desk():
    candidates = [
        {"desk_id": "DESK1_SCALPER", "symbol": "EURUSD", "direction": "LONG", "confidence": 0.7},
        {"desk_id": "DESK2_INTRADAY", "symbol": "EURUSD", "direction": "SHORT", "confidence": 0.7},
        {"desk_id": "DESK3_SWING", "symbol": "EURUSD", "direction": "LONG", "confidence": 0.8},
        {"desk_id": "DESK4_GOLD", "symbol": "XAUUSD", "direction": "LONG", "confidence": 0.7},
        {"desk_id": "DESK5_ALTS", "symbol": "BTCUSD", "direction": "LONG", "confidence": 0.7},
        {"desk_id": "DESK6_EQUITIES", "symbol": "AAPL", "direction": "LONG", "confidence": 0.7},
    ]

    e = CrossDeskBiasEngine()
    e.update_from_candidate(candidates[2])

    passed = []
    for c in candidates:
        out = e.apply(dict(c))
        if not out.get("blocked_by_bias"):
            passed.append(out)

    desks_passed = {p["desk_id"] for p in passed}
    for desk in ["DESK1_SCALPER", "DESK2_INTRADAY", "DESK3_SWING", "DESK5_ALTS", "DESK6_EQUITIES"]:
        assert desk in desks_passed

    gold = scan_gold_modes("XAUUSD", "TRENDING", spread_ok=True)
    assert len(gold) >= 1
