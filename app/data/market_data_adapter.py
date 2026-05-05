class MarketDataAdapter:
    def get_quote(self,symbol:str):
        return {'symbol':symbol,'price':0.0,'source':'mock'}
