"""
Autonomous Institutional Trading System - Phase 1
FastAPI Webhook Receiver for LuxAlgo TradingView Alerts
"""
import os
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.database import engine, Base, check_db_connection
from app.routes.webhook import router as webhook_router
from app.routes.health import router as health_router
from app.routes.dashboard import router as dashboard_router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("TradingSystem")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown lifecycle."""
    logger.info("=" * 60)
    logger.info("AUTONOMOUS TRADING SYSTEM - INITIALIZING")
    logger.info("=" * 60)

    # Create tables
    Base.metadata.create_all(bind=engine)
    logger.info("Database tables verified")

    # Verify DB connection
    if check_db_connection():
        logger.info("PostgreSQL connection confirmed")
    else:
        logger.error("PostgreSQL connection FAILED - system degraded")

    logger.info("Phase 1 Webhook Receiver ONLINE")
    logger.info("=" * 60)

    yield

    logger.info("Trading system shutting down")


app = FastAPI(
    title="Autonomous Institutional Trading System",
    description="Phase 1: Webhook Receiver & Signal Logger",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register routes
app.include_router(health_router, prefix="/api", tags=["Health"])
app.include_router(webhook_router, prefix="/api", tags=["Webhook"])
app.include_router(dashboard_router, prefix="/api", tags=["Dashboard"])
