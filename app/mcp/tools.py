from app.mcp.client import safe_call

def call_market_data_tool(symbol,timeframe): return safe_call('market_data',{'symbol':symbol,'timeframe':timeframe})
def call_historical_data_tool(symbol,start,end,timeframe): return safe_call('historical_ohlcv',{'symbol':symbol,'start':start,'end':end,'timeframe':timeframe})
def call_calendar_tool(): return safe_call('economic_calendar',{})
def call_sentiment_tool(query): return safe_call('news_sentiment',{'query':query})
def call_backtest_tool(strategy_config): return safe_call('backtest_runner',{'strategy_config':strategy_config})
