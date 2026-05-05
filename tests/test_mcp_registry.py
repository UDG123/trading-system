from app.mcp.registry import list_tools

def test_lists_tools():
    names=[t.name for t in list_tools()]
    assert 'market_data' in names

def test_execution_disabled_default():
    tool=[t for t in list_tools() if t.name=='broker_execution'][0]
    assert tool.enabled is False
