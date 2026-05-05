import asyncio
import logging
import os
from contextlib import asynccontextmanager

import redis.asyncio as aioredis
from fastapi import FastAPI

from app.api.routes_health import router as health_router
from app.api.routes_mcp import router as mcp_router
from app.api.routes_trades import router as trades_router
from app.api.routes_webhooks import router as webhooks_router
from app.config import REDIS_URL
from app.database import Base, engine, SessionLocal
from app.logging_config import configure_logging
from app.services.signal_engine.engine import SignalEngine

configure_logging()
logger = logging.getLogger("TradingSystem.API")
Base.metadata.create_all(bind=engine)


@asynccontextmanager
async def lifespan(app: FastAPI):
    redis_pool = None
    engine_task = None

    signal_source = os.getenv("SIGNAL_SOURCE", "BOTH").upper()
    if signal_source in ("PYTHON_ONLY", "BOTH"):
        try:
            logger.info("Starting OniQuant SignalEngine background task...")
            redis_pool = aioredis.from_url(
                REDIS_URL,
                decode_responses=False,
                max_connections=10,
            )
            signal_engine = SignalEngine(
                redis_pool=redis_pool,
                db_session_factory=SessionLocal,
            )
            engine_task = asyncio.create_task(
                signal_engine.run(),
                name="oniquant_signal_engine",
            )
            logger.info("SignalEngine background task started")
        except Exception as e:
            logger.exception("Failed to start SignalEngine: %s", e)

    try:
        yield
    finally:
        if engine_task:
            engine_task.cancel()
            try:
                await engine_task
            except asyncio.CancelledError:
                pass
        if redis_pool is not None:
            await redis_pool.aclose()


app = FastAPI(title="OniQuant Lux Prop Engine", lifespan=lifespan)
app.include_router(health_router)
app.include_router(webhooks_router)
app.include_router(trades_router)
app.include_router(mcp_router)
