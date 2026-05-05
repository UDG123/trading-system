from app.mcp.schemas import MCPTool
TOOLS=[
MCPTool(name='market_data',category='data',description='Quote lookup'),
MCPTool(name='historical_ohlcv',category='data',description='Historical bars'),
MCPTool(name='economic_calendar',category='data',description='Calendar events'),
MCPTool(name='news_sentiment',category='data',description='Sentiment'),
MCPTool(name='portfolio_analytics',category='analytics',description='Portfolio stats'),
MCPTool(name='backtest_runner',category='simulation',description='Backtest'),
MCPTool(name='broker_status',category='broker',description='Broker health'),
MCPTool(name='broker_execution',category='execution',description='Exec bridge',enabled=False,risk_level='execution',allowed_in_paper=False,allowed_in_live=False),
]

def list_tools():
    return TOOLS
