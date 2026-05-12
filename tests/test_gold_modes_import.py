def test_gold_modes_importable():
    import app.services.signal_engine.gold_modes as gm
    assert hasattr(gm, 'scan_gold_modes')
