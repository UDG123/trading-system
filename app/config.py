from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', extra='ignore')

    app_name: str = 'OniQuant Lux Prop Engine'
    env: str = 'dev'
    port: int = 8000
    database_url: str = 'sqlite:///./oniquant.db'
    redis_url: str = 'redis://localhost:6379/0'

    live_trading_enabled: bool = False
    mcp_execution_enabled: bool = False
    broker_api_key: str | None = None

    account_equity: float = 100000.0
    max_risk_per_trade_pct: float = 0.5
    max_daily_loss_pct: float = 3.0
    max_open_trades: int = 3
    max_trades_per_day: int = 10
    min_rr: float = 1.5

    enable_crypto_desk: bool = False
    enable_fx_exotics: bool = False

    telegram_bot_token: str | None = None
    telegram_chat_id: str | None = None


settings = Settings()
