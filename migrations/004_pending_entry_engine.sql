-- Migration 004: Pending Entry Engine
-- Parks CTO-approved signals for pullback entries when price is overextended.

CREATE TABLE IF NOT EXISTS pending_signals (
    id SERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    signal_id INTEGER NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    direction VARCHAR(10) NOT NULL,
    alert_type VARCHAR(50),
    desk_id VARCHAR(30) NOT NULL,
    timeframe VARCHAR(10),
    signal_price DOUBLE PRECISION NOT NULL,
    entry_target DOUBLE PRECISION NOT NULL,
    stop_loss DOUBLE PRECISION,
    take_profit_1 DOUBLE PRECISION,
    take_profit_2 DOUBLE PRECISION,
    entry_method VARCHAR(30),
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    ttl_expiry TIMESTAMPTZ NOT NULL,
    triggered_at TIMESTAMPTZ,
    trigger_price DOUBLE PRECISION,
    consensus_score INTEGER,
    ml_score DOUBLE PRECISION,
    enrichment_snapshot JSONB,
    check_count INTEGER DEFAULT 0,
    last_checked TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS ix_pending_status ON pending_signals (status, ttl_expiry);
CREATE INDEX IF NOT EXISTS ix_pending_symbol ON pending_signals (symbol, status);
