from uuid import uuid4
from app.core.event_models import OrderEvent

class VirtualBroker:
    def __init__(self):
        self.open_positions=[]
        self.history=[]

    def place_order(self, order:dict)->OrderEvent:
        evt=OrderEvent(order_id=str(uuid4()), status='filled', mode='paper', **order)
        self.open_positions.append(evt)
        self.history.append(evt)
        return evt

    def close_order(self, order_id:str, reason:str='manual'):
        self.open_positions=[o for o in self.open_positions if o.order_id!=order_id]
        return {'order_id':order_id,'reason':reason}

    def update_trade_mark_price(self, symbol:str, price:float):
        return {'symbol':symbol,'price':price}

    def get_open_positions(self):
        return self.open_positions

    def get_trade_history(self):
        return self.history
