from fastapi import APIRouter
from sqlalchemy import text
from app.core.redis_bus import get_redis
from app.db.session import engine
from app.config import settings

router=APIRouter()

@router.get('/health')
def health(): return {'status':'ok'}

@router.get('/ready')
def ready():
    db_ok=redis_ok=True
    try:
        with engine.connect() as c: c.execute(text('SELECT 1'))
    except Exception:
        db_ok=False
    try:
        get_redis().ping()
    except Exception:
        redis_ok=False
    return {'status':'ready' if db_ok and redis_ok else 'degraded','db':db_ok,'redis':redis_ok,'mode':'live' if settings.live_trading_enabled else 'paper'}
