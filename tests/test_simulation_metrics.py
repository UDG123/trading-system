from types import SimpleNamespace
from datetime import datetime, timedelta, timezone
from app.services.simulation_metrics import compute_simulation_metrics

def test_compute_metrics_basic():
    now=datetime.now(timezone.utc)
    positions=[SimpleNamespace(status='CLOSED',realized_pnl=10,r_multiple=1.2,max_favorable_pips=20,max_adverse_pips=5,entry_time=now-timedelta(minutes=10),exit_time=now,desk_id='D1',symbol='EURUSD',regime='R',bias_alignment='ALIGNED',bias_action='BOOSTED',desk_mode='M',strategy_mode='S'),SimpleNamespace(status='CLOSED',realized_pnl=-5,r_multiple=-1,max_favorable_pips=5,max_adverse_pips=15,entry_time=now-timedelta(minutes=20),exit_time=now,desk_id='D1',symbol='EURUSD',regime='R',bias_alignment='COUNTER',bias_action='REDUCED',desk_mode='M',strategy_mode='S')]
    snaps=[SimpleNamespace(equity=1000),SimpleNamespace(equity=900)]
    out=compute_simulation_metrics(positions,snaps)
    assert out['total_trades']==2
    assert out['win_rate']==50.0
