import numpy as np

from app.services.research_tools import randomize_trade_order, rolling_time_splits, inject_ohlc_noise_with_atr


def test_randomize_trade_order_preserves_values():
    vals = [1, -2, 3, 4]
    shuffled = randomize_trade_order(vals, seed=1)
    assert sorted(shuffled) == sorted(vals)


def test_rolling_time_splits_no_shuffle():
    splits = list(rolling_time_splits(100, train=50, test=20, step=10))
    assert splits[0] == ((0, 50), (50, 70))


def test_inject_ohlc_noise_shape():
    ohlc = np.array([[1.0, 1.2, 0.9, 1.1, 100]])
    noisy = inject_ohlc_noise_with_atr(ohlc, atr=0.1)
    assert noisy.shape == ohlc.shape
