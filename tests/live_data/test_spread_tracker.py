from app.services.live_data.spread_tracker import SpreadTracker


def test_spread_tracker_metrics():
    st = SpreadTracker()
    for i in range(1, 6):
        st.update("XAUUSD", 1999.0, 1999.0 + i * 0.1)
    assert st.current_spread("XAUUSD") > 0
    assert st.spread_bps("XAUUSD", 2000) > 0
    assert 0 < st.rolling_percentile("XAUUSD") <= 1


def test_spread_acceptance_mode_sensitive():
    st = SpreadTracker()
    st.update("XAUUSD", 2000.0, 2000.2)
    assert st.is_spread_acceptable("XAUUSD", "DESK4_GOLD", mode="GOLD_INTRADAY", mid_price=2000.1)
