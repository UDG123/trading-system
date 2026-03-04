"""
Phase 1 Tests - Webhook receiver and signal validation.
Run with: pytest tests/ -v
"""
import json
import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch

# Patch DATABASE_URL before importing app
import os
os.environ["DATABASE_URL"] = "sqlite:///./test.db"
os.environ["WEBHOOK_SECRET"] = "test-secret"

from app.main import app
from app.database import engine, Base
from app.services.signal_validator import SignalValidator
from app.schemas import TradingViewAlert

client = TestClient(app)


@pytest.fixture(autouse=True)
def setup_db():
    """Create tables before each test, drop after."""
    Base.metadata.create_all(bind=engine)
    yield
    Base.metadata.drop_all(bind=engine)


# ─────────────────────────────────────────────
# HEALTH CHECK
# ─────────────────────────────────────────────
def test_health_check():
    resp = client.get("/api/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] in ("operational", "degraded")
    assert "database" in data


# ─────────────────────────────────────────────
# WEBHOOK - VALID SIGNALS
# ─────────────────────────────────────────────
def _make_payload(**overrides):
    base = {
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
        "smart_trail": 1.1035,
    }
    base.update(overrides)
    return base


def test_valid_bullish_signal():
    resp = client.post("/api/webhook", json=_make_payload())
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "accepted"
    assert data["is_valid"] is True
    assert data["signal_id"] is not None
    assert "DESK1_SCALPER" in data["desks_matched"]


def test_valid_bearish_signal():
    payload = _make_payload(
        alert_type="bearish_confirmation",
        tp1=1.1020,
        tp2=1.0990,
        sl1=1.1080,
        sl2=1.1100,
    )
    resp = client.post("/api/webhook", json=payload)
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "accepted"
    assert data["is_valid"] is True


def test_gold_signal_routes_to_desk4():
    payload = _make_payload(
        symbol="XAUUSD",
        exchange="OANDA",
        alert_type="bullish_plus",
        price=2650.50,
        tp1=2670.00,
        tp2=2690.00,
        sl1=2640.00,
        sl2=2630.00,
    )
    resp = client.post("/api/webhook", json=payload)
    data = resp.json()
    assert data["is_valid"] is True
    assert "DESK4_GOLD" in data["desks_matched"]


def test_eurusd_routes_to_multiple_desks():
    """EURUSD is on both Desk 1 and Desk 2."""
    payload = _make_payload(
        symbol="EURUSD",
        exchange=None,
    )
    resp = client.post("/api/webhook", json=payload)
    data = resp.json()
    assert len(data["desks_matched"]) >= 2


# ─────────────────────────────────────────────
# WEBHOOK - REJECTED SIGNALS
# ─────────────────────────────────────────────
def test_wrong_secret():
    payload = _make_payload(secret="wrong-secret")
    resp = client.post("/api/webhook", json=payload)
    assert resp.status_code == 401


def test_invalid_json():
    resp = client.post(
        "/api/webhook",
        content="not json at all",
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 400


def test_unknown_symbol():
    payload = _make_payload(symbol="FAKEUSD", exchange=None)
    resp = client.post("/api/webhook", json=payload)
    data = resp.json()
    assert data["status"] == "rejected"
    assert data["is_valid"] is False


def test_bullish_sl_above_price():
    """SL above price on bullish = invalid."""
    payload = _make_payload(sl1=1.1100)  # price is 1.1050
    resp = client.post("/api/webhook", json=payload)
    data = resp.json()
    assert data["is_valid"] is False


def test_missing_sl_on_entry():
    """Entry signals require SL."""
    payload = _make_payload(sl1=None, sl2=None)
    resp = client.post("/api/webhook", json=payload)
    data = resp.json()
    assert data["is_valid"] is False


# ─────────────────────────────────────────────
# SIGNAL VALIDATOR UNIT TESTS
# ─────────────────────────────────────────────
def test_validator_unknown_alert_type():
    alert = TradingViewAlert(
        secret="x",
        symbol="EURUSD",
        timeframe="5M",
        alert_type="fake_signal",
        price=1.1050,
    )
    v = SignalValidator()
    valid, errors = v.validate(alert, "EURUSD", ["DESK1_SCALPER"])
    assert valid is False
    assert any("Unknown alert_type" in e for e in errors)


def test_validator_no_desk_match():
    alert = TradingViewAlert(
        secret="x",
        symbol="FAKEUSD",
        timeframe="5M",
        alert_type="bullish_confirmation",
        price=1.1050,
        sl1=1.1020,
    )
    v = SignalValidator()
    valid, errors = v.validate(alert, "FAKEUSD", [])
    assert valid is False
    assert any("does not match any desk" in e for e in errors)


# ─────────────────────────────────────────────
# DASHBOARD
# ─────────────────────────────────────────────
def test_dashboard():
    resp = client.get("/api/dashboard")
    assert resp.status_code == 200
    data = resp.json()
    assert data["firm_status"] == "OPERATIONAL"
    assert len(data["desks"]) == 6
