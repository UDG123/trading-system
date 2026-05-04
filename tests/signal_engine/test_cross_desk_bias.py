from app.services.signal_engine.cross_desk_bias import CrossDeskBiasEngine


def _seed_bias(engine):
    engine.update_from_candidate({
        "symbol": "EURUSD",
        "desk_id": "DESK3_SWING",
        "direction": "LONG",
        "confidence": 0.8,
    })


def test_no_bias_signal_passes():
    e = CrossDeskBiasEngine()
    c = {"symbol": "GBPUSD", "desk_id": "DESK2_INTRADAY", "direction": "LONG", "confidence": 0.6}
    out = e.apply(c)
    assert out["bias_alignment"] == "NEUTRAL"
    assert out["bias_action"] == "NO_BIAS"
    assert not out.get("blocked_by_bias", False)


def test_aligned_bias_boosts_confidence():
    e = CrossDeskBiasEngine()
    _seed_bias(e)
    out = e.apply({"symbol": "EURUSD", "desk_id": "DESK2_INTRADAY", "direction": "LONG", "confidence": 0.6})
    assert out["bias_alignment"] == "ALIGNED"
    assert out["bias_action"] == "BOOSTED"
    assert out["confidence"] > 0.6


def test_counter_desk2_soft_reduces_confidence(monkeypatch):
    monkeypatch.setenv("ENABLE_HARD_BIAS_FILTER", "false")
    e = CrossDeskBiasEngine()
    _seed_bias(e)
    out = e.apply({"symbol": "EURUSD", "desk_id": "DESK2_INTRADAY", "direction": "SHORT", "confidence": 0.8})
    assert out["bias_action"] == "REDUCED"
    assert out["confidence"] < 0.8
    assert not out.get("blocked_by_bias", False)


def test_counter_desk2_hard_blocks(monkeypatch):
    monkeypatch.setenv("ENABLE_HARD_BIAS_FILTER", "true")
    e = CrossDeskBiasEngine()
    _seed_bias(e)
    out = e.apply({"symbol": "EURUSD", "desk_id": "DESK2_INTRADAY", "direction": "SHORT", "confidence": 0.8})
    assert out["bias_action"] == "BLOCKED"
    assert out.get("blocked_by_bias", False)


def test_counter_desk1_hard_not_blocked(monkeypatch):
    monkeypatch.setenv("ENABLE_HARD_BIAS_FILTER", "true")
    e = CrossDeskBiasEngine()
    _seed_bias(e)
    out = e.apply({"symbol": "EURUSD", "desk_id": "DESK1_SCALPER", "direction": "SHORT", "confidence": 0.8})
    assert out["bias_action"] == "REDUCED"
    assert not out.get("blocked_by_bias", False)
