"""
Phase 3 Tests - Trade queue API endpoints for VPS integration.
"""
import json
import pytest
import os

os.environ["DATABASE_URL"] = "sqlite:///./test_phase3.db"
os.environ["WEBHOOK_SECRET"] = "test-secret"

from fastapi.testclient import TestClient
from app.main import app
from app.database import Base, engine

client = TestClient(app)

@pytest.fixture(autouse=True)
def setup_db():
    Base.metadata.create_all(bind=engine)
    yield
    Base.metadata.drop_all(bind=engine)


def _create_decided_signal():
    """Helper: create a signal that's been through the pipeline and approved."""
    resp = client.post("/api/webhook", json={
        "secret": "test-secret",
        "symbol": "EURUSD",
        "exchange": "FX",
        "timeframe": "5M",
        "alert_type": "bullish_confirmation",
        "price": 1.1050,
        "tp1": 1.1080,
        "tp2": 1.1120,
        "sl1": 1.1020,
        "sl2": 1.1000,
    })
    assert resp.status_code == 200
    signal_id = resp.json()["signal_id"]

    from app.database import SessionLocal
    from app.models.signal import Signal
    db = SessionLocal()
    signal = db.query(Signal).filter(Signal.id == signal_id).first()
    signal.status = "DECIDED"
    signal.claude_decision = "EXECUTE"
    signal.consensus_score = 8
    signal.ml_score = 0.72
    signal.position_size_pct = 0.5
    signal.desk_id = "DESK1_SCALPER"
    db.commit()
    db.close()

    return signal_id


def test_pending_trades_empty():
    resp = client.get("/api/trades/pending", headers={"X-API-Key": "test-secret"})
    assert resp.status_code == 200
    assert resp.json()["count"] == 0


def test_pending_trades_auth_required():
    resp = client.get("/api/trades/pending", headers={"X-API-Key": "wrong-key"})
    assert resp.status_code == 401


def test_pending_trades_returns_decided():
    signal_id = _create_decided_signal()
    resp = client.get("/api/trades/pending", headers={"X-API-Key": "test-secret"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["count"] == 1
    trade = data["pending"][0]
    assert trade["signal_id"] == signal_id
    assert trade["symbol"] == "EURUSD"
    assert trade["direction"] == "LONG"
    assert trade["desk_id"] == "DESK1_SCALPER"


def test_pending_trade_marked_queued():
    _create_decided_signal()
    resp1 = client.get("/api/trades/pending", headers={"X-API-Key": "test-secret"})
    assert resp1.json()["count"] == 1
    resp2 = client.get("/api/trades/pending", headers={"X-API-Key": "test-secret"})
    assert resp2.json()["count"] == 0


def test_report_execution():
    signal_id = _create_decided_signal()
    client.get("/api/trades/pending", headers={"X-API-Key": "test-secret"})

    resp = client.post("/api/trades/executed", json={
        "signal_id": signal_id,
        "desk_id": "DESK1_SCALPER",
        "symbol": "EURUSD",
        "direction": "LONG",
        "mt5_ticket": 12345678,
        "entry_price": 1.1052,
        "lot_size": 0.15,
        "stop_loss": 1.1020,
        "take_profit": 1.1080,
    }, headers={"X-API-Key": "test-secret"})

    assert resp.status_code == 200
    assert resp.json()["status"] == "recorded"

    sig_resp = client.get(f"/api/signal/{signal_id}")
    assert sig_resp.json()["status"] == "EXECUTED"


def test_report_close():
    signal_id = _create_decided_signal()
    client.get("/api/trades/pending", headers={"X-API-Key": "test-secret"})

    client.post("/api/trades/executed", json={
        "signal_id": signal_id,
        "desk_id": "DESK1_SCALPER",
        "symbol": "EURUSD",
        "direction": "LONG",
        "mt5_ticket": 99999,
        "entry_price": 1.1052,
        "lot_size": 0.15,
        "stop_loss": 1.1020,
        "take_profit": 1.1080,
    }, headers={"X-API-Key": "test-secret"})

    resp = client.post("/api/trades/closed", json={
        "mt5_ticket": 99999,
        "exit_price": 1.1078,
        "pnl_dollars": 39.0,
        "pnl_pips": 26.0,
        "close_reason": "TP",
    }, headers={"X-API-Key": "test-secret"})

    assert resp.status_code == 200
    assert resp.json()["pnl"] == 39.0


def test_open_trades_listing():
    signal_id = _create_decided_signal()
    client.get("/api/trades/pending", headers={"X-API-Key": "test-secret"})

    client.post("/api/trades/executed", json={
        "signal_id": signal_id,
        "desk_id": "DESK1_SCALPER",
        "symbol": "EURUSD",
        "direction": "LONG",
        "mt5_ticket": 55555,
        "entry_price": 1.1052,
        "lot_size": 0.15,
        "stop_loss": 1.1020,
    }, headers={"X-API-Key": "test-secret"})

    resp = client.get("/api/trades/open", headers={"X-API-Key": "test-secret"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["count"] == 1
    assert data["open_trades"][0]["mt5_ticket"] == 55555


def test_full_trade_lifecycle():
    signal_id = _create_decided_signal()

    r1 = client.get("/api/trades/pending", headers={"X-API-Key": "test-secret"})
    assert r1.json()["count"] == 1

    r2 = client.post("/api/trades/executed", json={
        "signal_id": signal_id,
        "desk_id": "DESK1_SCALPER",
        "symbol": "EURUSD",
        "direction": "LONG",
        "mt5_ticket": 77777,
        "entry_price": 1.1052,
        "lot_size": 0.10,
        "stop_loss": 1.1020,
        "take_profit": 1.1080,
    }, headers={"X-API-Key": "test-secret"})
    assert r2.json()["status"] == "recorded"

    r3 = client.get("/api/trades/open", headers={"X-API-Key": "test-secret"})
    assert r3.json()["count"] == 1

    r4 = client.post("/api/trades/closed", json={
        "mt5_ticket": 77777,
        "exit_price": 1.1080,
        "pnl_dollars": 28.0,
        "close_reason": "TP",
    }, headers={"X-API-Key": "test-secret"})
    assert r4.json()["status"] == "recorded"

    r5 = client.get("/api/trades/open", headers={"X-API-Key": "test-secret"})
    assert r5.json()["count"] == 0

    r6 = client.get(f"/api/signal/{signal_id}")
    assert r6.json()["status"] == "CLOSED"
