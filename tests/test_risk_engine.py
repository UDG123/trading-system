from app.core.risk_engine import RiskEngine
from app.core.event_models import SignalEvent

r=RiskEngine()

def test_reject_missing_sl():
    s=SignalEvent(event_id='1',source='tv',symbol='XAUUSD',timeframe='5m',side='buy',entry=1,stop_loss=None,take_profit=2,raw_payload={})
    ok,reasons,_=r.evaluate(s)
    assert not ok and 'missing_stop_loss' in reasons

def test_reject_bad_rr():
    s=SignalEvent(event_id='1',source='tv',symbol='XAUUSD',timeframe='5m',side='buy',entry=10,stop_loss=9.9,take_profit=10.01,raw_payload={})
    ok,reasons,_=r.evaluate(s)
    assert not ok and 'invalid_rr' in reasons
