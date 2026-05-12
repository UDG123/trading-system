import json
from app.services.signal_engine.mean_reversion import MeanReversionStrategy
from app.services.signal_engine.regime_detector import HMMRegimeDetector


def test_keltner_squeeze_and_volume_divergence_long_signal():
    s = MeanReversionStrategy()
    indicators = {
        "price": 99.8, "prev_price": 100.2,
        "bb_lower": 99.7, "bb_upper": 100.3, "bb_mid": 100.0,
        "kc_lower": 99.4, "kc_upper": 100.6,
        "rsi": 30,
        "volume": 800, "avg_volume": 1200,
    }
    out = s.evaluate("EURUSD", "15M", "DESK2_INTRADAY", indicators)
    assert out is not None
    assert out["direction"] == "LONG"
    assert out["keltner_squeeze"] is True
    assert out["volume_divergence"] == "bullish"


def test_keltner_squeeze_detect_false_when_bb_wide():
    s = MeanReversionStrategy()
    indicators = {"bb_lower": 98, "bb_upper": 102, "kc_lower": 99, "kc_upper": 101}
    assert s.detect_keltner_squeeze(indicators, squeeze_threshold=1.0) is False


def test_regime_probability_size_adjustment():
    assert HMMRegimeDetector._probability_size_adjustment({"TRENDING_UP": 0.5, "TRENDING_DOWN": 0.25, "RANGING": 0.25}) > 1
    assert HMMRegimeDetector._probability_size_adjustment({"TRENDING_UP": 0.1, "TRENDING_DOWN": 0.1, "RANGING": 0.8}) < 1
