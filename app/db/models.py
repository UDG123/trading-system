from sqlalchemy import Column, DateTime, Float, Integer, String, JSON, Boolean
from sqlalchemy.sql import func
from app.db.session import Base

class Signal(Base):
    __tablename__='signals'
    id=Column(Integer, primary_key=True)
    event_id=Column(String, unique=True)
    symbol=Column(String)
    desk=Column(String)
    payload=Column(JSON)
    created_at=Column(DateTime, server_default=func.now())

class ValidatedSignalModel(Base):
    __tablename__='validated_signals'
    id=Column(Integer, primary_key=True)
    event_id=Column(String)
    approved=Column(Boolean)
    score=Column(Float)
    reasons=Column(JSON)
    created_at=Column(DateTime, server_default=func.now())

class Order(Base):
    __tablename__='orders'
    id=Column(Integer, primary_key=True)
    order_id=Column(String, unique=True)
    signal_id=Column(String)
    symbol=Column(String)
    side=Column(String)
    qty=Column(Float)
    status=Column(String)
    mode=Column(String)
    created_at=Column(DateTime, server_default=func.now())

class Trade(Base):
    __tablename__='trades'
    id=Column(Integer, primary_key=True)
    order_id=Column(String)
    symbol=Column(String)
    pnl=Column(Float, default=0)
    status=Column(String, default='open')
    created_at=Column(DateTime, server_default=func.now())

class RiskEvent(Base):
    __tablename__='risk_events'
    id=Column(Integer, primary_key=True)
    event_id=Column(String)
    approved=Column(Boolean)
    reason=Column(String)
    created_at=Column(DateTime, server_default=func.now())

class MCPCall(Base):
    __tablename__='mcp_calls'
    id=Column(Integer, primary_key=True)
    tool_name=Column(String)
    success=Column(Boolean)
    details=Column(JSON)
    created_at=Column(DateTime, server_default=func.now())

class SystemEvent(Base):
    __tablename__='system_events'
    id=Column(Integer, primary_key=True)
    kind=Column(String)
    payload=Column(JSON)
    created_at=Column(DateTime, server_default=func.now())
