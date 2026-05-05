from collections import defaultdict

def compute_simulation_metrics(sim_positions, sim_equity_snapshots):
    closed=[p for p in sim_positions if getattr(p,'status',None)=='CLOSED']
    total=len(closed)
    pnls=[float(getattr(p,'realized_pnl',0) or 0) for p in closed]
    wins=[p for p in pnls if p>0]; losses=[p for p in pnls if p<0]
    gross_win=sum(wins); gross_loss=abs(sum(losses))
    win_rate=(len(wins)/total*100) if total else 0
    avg_win=(gross_win/len(wins)) if wins else 0
    avg_loss=(sum(losses)/len(losses)) if losses else 0
    expectancy=((len(wins)/total)*avg_win + (len(losses)/total)*avg_loss) if total else 0
    pf=(gross_win/gross_loss) if gross_loss else 0
    hold_w=[]; hold_l=[]
    by=defaultdict(lambda: {'trades':0,'pnl':0})
    for p in closed:
      pnl=float(getattr(p,'realized_pnl',0) or 0)
      if getattr(p,'entry_time',None) and getattr(p,'exit_time',None):
        mins=(p.exit_time-p.entry_time).total_seconds()/60
        (hold_w if pnl>0 else hold_l).append(mins)
      keys=['desk_id','symbol','regime','bias_alignment','bias_action','desk_mode','strategy_mode']
      for k in keys:
        g=f'{k}:{getattr(p,k,None)}'
        by[g]['trades']+=1; by[g]['pnl']+=pnl
    dd=0
    if sim_equity_snapshots:
      peak=-1e18
      for s in sim_equity_snapshots:
        e=float(getattr(s,'equity',0) or 0)
        peak=max(peak,e)
        if peak>0: dd=max(dd,(peak-e)/peak*100)
    return {
      'total_trades': total,'win_rate': round(win_rate,2),'expectancy': round(expectancy,2),'profit_factor': round(pf,3),
      'avg_r_multiple': round(sum(float(getattr(p,'r_multiple',0) or 0) for p in closed)/total,3) if total else 0,
      'max_drawdown_pct': round(dd,2),'avg_winner_hold_mins': round(sum(hold_w)/len(hold_w),2) if hold_w else 0,
      'avg_loser_hold_mins': round(sum(hold_l)/len(hold_l),2) if hold_l else 0,
      'mfe_average': round(sum(float(getattr(p,'max_favorable_pips',0) or 0) for p in closed)/total,2) if total else 0,
      'mae_average': round(sum(float(getattr(p,'max_adverse_pips',0) or 0) for p in closed)/total,2) if total else 0,
      'performance_breakdown': dict(by)
    }
