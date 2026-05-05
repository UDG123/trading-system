from app.core.desk_scanner import classify_symbol
from app.core.signal_validator import session_filter, spread_check
from app.core.risk_engine import RiskEngine
from app.core.event_models import ValidatedSignal, SignalEvent

risk_engine = RiskEngine()

def run_pipeline(signal: SignalEvent)->ValidatedSignal:
    reasons=[]
    if classify_symbol(signal.symbol) is None:
        reasons.append('unsupported_symbol')
    if not session_filter(signal):
        reasons.append('session_blocked')
    if not spread_check(signal):
        reasons.append('spread_check_failed')
    approved,rr, snapshot = risk_engine.evaluate(signal)
    reasons.extend(rr)
    ok = len(reasons)==0 and approved
    return ValidatedSignal(original_event=signal, validation_score=1.0 if ok else 0.0, approved=ok, rejection_reasons=reasons, risk_snapshot=snapshot)
