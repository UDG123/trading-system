import importlib


def test_env_thresholds_exist():
    cfg = importlib.import_module("app.config")
    assert isinstance(cfg.QUALITY_SCORE_THRESHOLD, int)
    assert isinstance(cfg.MIN_CONFLUENCE_SCORE, float)
    assert isinstance(cfg.MIN_TIMEFRAME_BARS, int)
