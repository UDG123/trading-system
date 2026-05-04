import pandas as pd
from app.services.signal_engine.desk_scanner import DeskScanner


class _CM:
    def get_dataframe(self, symbol, tf):
        n = 120
        return pd.DataFrame({"close": [1.1 + i*0.0001 for i in range(n)], "high": [1.2]*n, "low": [1.0]*n})


def test_non_gold_scan_runs():
    ds = DeskScanner(_CM())
    out = ds.scan_desk("DESK1_SCALPER", regime_cache={"EURUSD": "TRENDING"})
    assert isinstance(out, list)
