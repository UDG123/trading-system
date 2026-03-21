-- Migration 003: Backtest Results table for strategy stress testing
-- OniQuant v5.9 — Zero-Key Simulation virtual ledger

CREATE TABLE IF NOT EXISTS backtest_results (
    id              SERIAL PRIMARY KEY,
    run_id          VARCHAR(64)     NOT NULL,
    run_timestamp   TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    -- Signal info
    symbol          VARCHAR(20)     NOT NULL,
    timeframe       VARCHAR(10)     NOT NULL DEFAULT '15M',
    direction       VARCHAR(10)     NOT NULL,
    signal_type     VARCHAR(50)     NOT NULL,
    signal_timestamp TIMESTAMPTZ    NOT NULL,

    -- Price levels
    entry_price     DOUBLE PRECISION NOT NULL,
    stop_loss       DOUBLE PRECISION NOT NULL,
    take_profit     DOUBLE PRECISION NOT NULL,
    exit_price      DOUBLE PRECISION,

    -- Hurst Gate
    hurst_exponent  DOUBLE PRECISION,
    hurst_vetoed    BOOLEAN         NOT NULL DEFAULT FALSE,

    -- Outcome
    outcome         VARCHAR(10),
    pnl_pips        DOUBLE PRECISION,
    pnl_dollars     DOUBLE PRECISION,
    risk_reward     DOUBLE PRECISION,

    -- Context
    rsi_at_entry    DOUBLE PRECISION,
    ema_fast        DOUBLE PRECISION,
    ema_slow        DOUBLE PRECISION,
    atr_at_entry    DOUBLE PRECISION,

    -- Audit
    notes           TEXT
);

CREATE INDEX IF NOT EXISTS ix_backtest_run_id ON backtest_results(run_id);
CREATE INDEX IF NOT EXISTS ix_backtest_run_symbol ON backtest_results(run_id, symbol);
