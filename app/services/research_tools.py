from __future__ import annotations

import random
from typing import Iterable
import numpy as np


def randomize_trade_order(trade_pnls: Iterable[float], seed: int = 42) -> list[float]:
    arr = list(trade_pnls)
    rng = random.Random(seed)
    rng.shuffle(arr)
    return arr


def shuffled_returns(returns: Iterable[float], seed: int = 42) -> list[float]:
    return randomize_trade_order(returns, seed)


def inject_ohlc_noise_with_atr(ohlc: np.ndarray, atr: float, noise_scale: float = 0.15, seed: int = 42) -> np.ndarray:
    rng = np.random.default_rng(seed)
    noisy = ohlc.copy().astype(float)
    noise = rng.normal(0, atr * noise_scale, size=noisy.shape)
    noisy[:, :4] = noisy[:, :4] + noise[:, :4]
    noisy[:, 1] = np.maximum(noisy[:, 1], noisy[:, [0, 3]].max(axis=1))
    noisy[:, 2] = np.minimum(noisy[:, 2], noisy[:, [0, 1, 3]].min(axis=1))
    return noisy


def rolling_time_splits(n: int, train: int, test: int, step: int):
    i = 0
    while i + train + test <= n:
        yield (i, i + train), (i + train, i + train + test)
        i += step
