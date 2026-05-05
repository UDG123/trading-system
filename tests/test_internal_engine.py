import asyncio
from fastapi.testclient import TestClient

from app.main import app
from app.services.data_providers.mock_provider import MockProvider
from app.services.signal_engine.candle_manager import CandleManager
from app.services.signal_engine.desk_scanner import DeskScanner


def test_candle_manager_accepts_provider_candles():
    async def run():
        provider = MockProvider()
        candles = await provider.get_candles("EURUSD", "1M", 60)
        cm = CandleManager()
        assert cm.update_dataframe("EURUSD", "1M", candles) == 60
        df = cm.get_dataframe("EURUSD", "1M")
        assert df is not None
        assert len(df) == 60
        assert float(df["close"].iloc[-1]) > 0
    asyncio.run(run())


def test_tradingview_disabled_does_not_break_startup():
    client = TestClient(app)
    response = client.post("/webhook/tradingview", json={"symbol": "EURUSD"})
    assert response.status_code == 200
    assert response.json()["status"] == "disabled"


def test_xauusd_gold_mode_metadata_and_non_xauusd_exclusion(monkeypatch):
    async def run():
        import app.services.signal_engine.desk_scanner as desk_scanner_module
        monkeypatch.setattr(desk_scanner_module, "is_valid_trading_hour", lambda *args, **kwargs: True)
        provider = MockProvider()
        cm = CandleManager()
        cm.update_dataframe("XAUUSD", "1M", await provider.get_candles("XAUUSD", "1M", 300))
        cm.update_dataframe("EURUSD", "1M", await provider.get_candles("EURUSD", "1M", 300))
        scanner = DeskScanner(cm)
        gold = scanner.scan_desk("DESK4_GOLD")
        assert gold
        assert all(c["symbol"] == "XAUUSD" for c in gold)
        assert {c["desk_mode"] for c in gold}.issubset({"GOLD_SCALP", "GOLD_INTRADAY", "GOLD_SWING"})
        assert all(c.get("strategy_mode") for c in gold)
        assert not [c for c in gold if c["symbol"] != "XAUUSD"]
    asyncio.run(run())
