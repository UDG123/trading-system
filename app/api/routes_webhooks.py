from uuid import uuid4
from fastapi import APIRouter
from app.core.event_models import SignalEvent
from app.core.redis_bus import publish_event, STREAMS

router=APIRouter(prefix='/webhook')

@router.post('/tradingview')
def tradingview(payload:dict):
    payload.setdefault('event_id',str(uuid4()))
    payload.setdefault('source','tradingview')
    payload.setdefault('raw_payload',payload.copy())
    evt=SignalEvent(**payload)
    publish_event(STREAMS['raw'], evt.model_dump(mode='json'))
    return {'status':'accepted','event_id':evt.event_id}
