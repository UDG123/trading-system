from datetime import datetime
from app.core.event_models import SignalEvent

def schema_validate(payload:dict)->SignalEvent:
    return SignalEvent(**payload)

def session_filter(_signal:SignalEvent)->bool:
    _ = datetime.utcnow()
    return True

def spread_check(_signal:SignalEvent)->bool:
    return True
