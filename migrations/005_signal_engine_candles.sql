-- Migration 005: Signal Engine OHLCV Tables
-- Higher timeframe candle storage for Python signal engine.
-- 1M already exists in ohlcv_1m.

CREATE TABLE IF NOT EXISTS ohlcv_5m (
    time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    PRIMARY KEY (time, symbol)
);
CREATE INDEX IF NOT EXISTS ix_ohlcv_5m_symbol_time ON ohlcv_5m (symbol, time);

CREATE TABLE IF NOT EXISTS ohlcv_15m (
    time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    PRIMARY KEY (time, symbol)
);
CREATE INDEX IF NOT EXISTS ix_ohlcv_15m_symbol_time ON ohlcv_15m (symbol, time);

CREATE TABLE IF NOT EXISTS ohlcv_1h (
    time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    PRIMARY KEY (time, symbol)
);
CREATE INDEX IF NOT EXISTS ix_ohlcv_1h_symbol_time ON ohlcv_1h (symbol, time);

CREATE TABLE IF NOT EXISTS ohlcv_4h (
    time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    PRIMARY KEY (time, symbol)
);
CREATE INDEX IF NOT EXISTS ix_ohlcv_4h_symbol_time ON ohlcv_4h (symbol, time);

CREATE TABLE IF NOT EXISTS ohlcv_1d (
    time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    PRIMARY KEY (time, symbol)
);
CREATE INDEX IF NOT EXISTS ix_ohlcv_1d_symbol_time ON ohlcv_1d (symbol, time);

CREATE TABLE IF NOT EXISTS ohlcv_1w (
    time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    PRIMARY KEY (time, symbol)
);
CREATE INDEX IF NOT EXISTS ix_ohlcv_1w_symbol_time ON ohlcv_1w (symbol, time);
