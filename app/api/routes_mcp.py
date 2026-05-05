from fastapi import APIRouter
from app.config import settings
from app.mcp.registry import list_tools
from app.core.redis_bus import get_redis
from app.db.session import engine

router=APIRouter()

@router.get('/mcp/tools')
def mcp_tools(): return list_tools()

@router.get('/system/status')
def status():
    db_ok=redis_ok=True
    try: engine.connect().close()
    except Exception: db_ok=False
    try: get_redis().ping()
    except Exception: redis_ok=False
    return {'mode':'live' if settings.live_trading_enabled else 'paper','db':db_ok,'redis':redis_ok,'workers':{}}
