from app.config import settings
from app.core.event_models import SignalEvent

class RiskEngine:
    def evaluate(self, signal: SignalEvent, open_trades:int=0, trades_today:int=0, daily_loss_pct:float=0.0):
        reasons=[]
        if signal.stop_loss is None:
            reasons.append('missing_stop_loss')
        else:
            risk=abs(signal.entry-signal.stop_loss)
            reward=abs(signal.take_profit-signal.entry)
            if risk<=0 or (reward/risk)<settings.min_rr:
                reasons.append('invalid_rr')
        if daily_loss_pct>=settings.max_daily_loss_pct:
            reasons.append('daily_loss_exceeded')
        if open_trades>=settings.max_open_trades:
            reasons.append('max_open_trades')
        if trades_today>=settings.max_trades_per_day:
            reasons.append('max_trades_per_day')
        approved = len(reasons)==0
        qty=0.0
        if approved:
            risk_amount=settings.account_equity*(settings.max_risk_per_trade_pct/100)
            per_unit=abs(signal.entry-signal.stop_loss)
            qty=round(risk_amount/per_unit,4) if per_unit>0 else 0
        return approved,reasons,{'qty':qty}
