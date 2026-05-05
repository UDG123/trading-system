from fastapi import FastAPI
from app.logging_config import configure_logging
from app.db.session import Base, engine
from app.api.routes_health import router as health_router
from app.api.routes_webhooks import router as webhooks_router
from app.api.routes_trades import router as trades_router
from app.api.routes_mcp import router as mcp_router

configure_logging()
Base.metadata.create_all(bind=engine)
app=FastAPI(title='OniQuant Lux Prop Engine')
app.include_router(health_router)
app.include_router(webhooks_router)
app.include_router(trades_router)
app.include_router(mcp_router)
