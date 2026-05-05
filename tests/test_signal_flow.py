from fastapi.testclient import TestClient
from app.main import app
from app.core.pipeline_v2 import run_pipeline
from app.core.event_models import SignalEvent

client=TestClient(app)

def test_webhook_accepts_signal():
    payload={"symbol":"XAUUSD","timeframe":"5m","side":"buy","entry":2300,"stop_loss":2295,"take_profit":2310,"confidence":0.8,"strategy_name":"tv"}
    r=client.post('/webhook/tradingview',json=payload)
    assert r.status_code==200
    assert r.json()['status']=='accepted'

def test_invalid_signal_rejected():
    s=SignalEvent(event_id='1',source='tv',symbol='BAD',timeframe='5m',side='buy',entry=1,stop_loss=0.5,take_profit=2,raw_payload={})
    v=run_pipeline(s)
    assert not v.approved
