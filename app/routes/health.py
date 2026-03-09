"""
Health Check Route - System status and database connectivity.
"""
import time
import logging
from datetime import datetime, timezone, timedelta

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import func

from app.database import get_db, check_db_connection
from app.schemas import HealthResponse
from app.models.signal import Signal

logger = logging.getLogger("TradingSystem.Health")
router = APIRouter()

_start_time = time.time()


@router.get("/health", response_model=HealthResponse)
async def health_check(db: Session = Depends(get_db)):
    """System health check - used by Railway and monitoring."""
    db_ok = check_db_connection()

    # Count today's signals
    today_start = datetime.now(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    try:
        signals_today = (
            db.query(func.count(Signal.id))
            .filter(Signal.received_at >= today_start)
            .scalar()
        ) or 0
    except Exception:
        signals_today = -1

    return HealthResponse(
        status="operational" if db_ok else "degraded",
        database="connected" if db_ok else "disconnected",
        uptime_seconds=round(time.time() - _start_time, 1),
        signals_today=signals_today,
    )
