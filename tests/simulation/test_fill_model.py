from app.services.simulation.fill_model import estimate_fill, resolve_same_bar_exit


def test_buy_uses_ask_sell_uses_bid():
    b = estimate_fill("BUY", bid=99, ask=100)
    s = estimate_fill("SELL", bid=99, ask=100)
    assert b.fill_price > 100
    assert s.fill_price < 99


def test_same_bar_conservative_rule():
    out = resolve_same_bar_exit("LONG", bar_low=95, bar_high=110, stop_loss=96, take_profit=108)
    assert out == "SL"
