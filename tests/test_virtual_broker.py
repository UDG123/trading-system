from app.brokers.virtual_broker import VirtualBroker

def test_place_paper_order():
    b=VirtualBroker()
    e=b.place_order({'signal_id':'s1','symbol':'XAUUSD','side':'buy','qty':1,'entry':1,'stop_loss':0.5,'take_profit':2})
    assert e.mode=='paper'
    assert len(b.get_open_positions())==1
