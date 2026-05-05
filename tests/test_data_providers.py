import asyncio

from app.services.data_providers.base import get_market_data_provider
from app.services.data_providers.mock_provider import MockProvider


def test_mock_provider_candle_and_quote_format():
    async def run():
        provider = MockProvider()
        candles = await provider.get_candles("EURUSD", "1M", 5)
        assert len(candles) == 5
        candle = candles[-1]
        assert set(["time", "open", "high", "low", "close", "volume", "provider"]).issubset(candle)
        assert isinstance(candle["close"], float)
        quote = await provider.get_quote("EURUSD")
        assert quote["symbol"] == "EURUSD"
        assert quote["mid"] > 0
        assert quote["provider"] == "mock"
    asyncio.run(run())


def test_missing_twelvedata_api_key_falls_back_to_mock(monkeypatch):
    monkeypatch.setattr("app.services.data_providers.base.DATA_PROVIDER", "TWELVEDATA")
    monkeypatch.setattr("app.services.data_providers.base.TWELVEDATA_API_KEY", "")
    provider = get_market_data_provider()
    assert isinstance(provider, MockProvider)
