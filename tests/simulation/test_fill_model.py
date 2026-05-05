from app.services.simulation.fill_model import estimate_entry_fill, evaluate_bar_exit, simulate_trade_path


def test_buy_uses_ask_sell_uses_bid():
    b = estimate_entry_fill("BUY", bid=99, ask=100)
    s = estimate_entry_fill("SELL", bid=99, ask=100)
    assert b["entry_fill_price"] > 100
    assert s["entry_fill_price"] < 99


def test_same_bar_conservative_rule():
    out = evaluate_bar_exit("LONG", bar_low=95, bar_high=110, stop_loss=96, take_profit=108)
    assert out["exit_reason"] == "SL"
    assert out["pessimistic_same_bar_applied"] is True


def test_simulate_trade_path_sl_first():
    res = simulate_trade_path("LONG", 100, 98, 104, [{"low": 97, "high": 105}])
    assert res["exit_reason"] == "SL"
