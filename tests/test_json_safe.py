import json
from decimal import Decimal
from datetime import datetime
import math

import numpy as np
import pandas as pd

from app.utils.json_safe import dumps_json_safe, to_json_safe


def test_numpy_and_pandas_json_safe():
    payload = {
        np.int64(1): np.float64(2.5),
        "ts": pd.Timestamp("2025-01-01T00:00:00Z"),
        "nan": float("nan"),
        "inf": np.float64(math.inf),
        "arr": np.array([1, 2]),
        "d": Decimal("3.14"),
        "dt": datetime(2025, 1, 1),
        "nested": [{"v": np.float64(4.2)}],
    }
    safe = to_json_safe(payload)
    assert safe["1"] == 2.5
    assert safe["nan"] is None
    assert safe["inf"] is None
    out = dumps_json_safe(payload)
    parsed = json.loads(out)
    assert parsed["arr"] == [1, 2]
    assert parsed["nested"][0]["v"] == 4.2


def test_sample_candidate_payload_serializes():
    payload = {"event_type": "candidate.generated", "desk": "DESK5_ALTS", "symbol": "BTCUSD", "probability": np.float64(0.0664)}
    assert "BTCUSD" in dumps_json_safe(payload)
