-- ============================================================
-- Shadow Sim Engine Migration
-- ML data collection engine and simulation environment.
-- 5 new tables + 1 reference table for realistic spread modeling.
-- ============================================================

-- ──────────────────────────────────────────────────────────────
-- 1. shadow_signals — logs EVERY incoming signal with full
--    feature context regardless of gate decisions
-- ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS shadow_signals (
    id              SERIAL PRIMARY KEY,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Signal identity
    symbol          VARCHAR(20)  NOT NULL,
    direction       VARCHAR(10),
    alert_type      VARCHAR(50),
    timeframe       VARCHAR(10),
    desk_id         VARCHAR(30),
    price           DOUBLE PRECISION,

    -- LuxAlgo levels
    sl1             DOUBLE PRECISION,
    tp1             DOUBLE PRECISION,
    tp2             DOUBLE PRECISION,
    smart_trail     DOUBLE PRECISION,

    -- Gate values logged as FEATURES (not filters)
    hurst_exponent      DOUBLE PRECISION,
    indicator_alignment DOUBLE PRECISION,   -- 0.0-1.0
    consensus_score     INTEGER,
    consensus_tier      VARCHAR(10),
    ml_score            DOUBLE PRECISION,
    claude_decision     VARCHAR(20),
    claude_confidence   DOUBLE PRECISION,
    claude_reasoning    TEXT,

    -- "Would block" flags
    hurst_would_block       BOOLEAN,
    alignment_would_block   BOOLEAN,
    consensus_would_block   BOOLEAN,
    cto_would_skip          BOOLEAN,
    live_pipeline_approved  BOOLEAN,

    -- Market context
    rsi                 DOUBLE PRECISION,
    adx                 DOUBLE PRECISION,
    atr                 DOUBLE PRECISION,
    atr_pct             DOUBLE PRECISION,
    ema50               DOUBLE PRECISION,
    ema200              DOUBLE PRECISION,
    trend               VARCHAR(20),
    volatility_regime   VARCHAR(20),
    active_session      VARCHAR(30),
    kill_zone_type      VARCHAR(20),
    vix_level           DOUBLE PRECISION,
    dxy_change_pct      DOUBLE PRECISION,

    -- Cross-asset
    correlated_open_count   INTEGER,
    correlation_group       VARCHAR(30),

    -- Volume
    volume              DOUBLE PRECISION,
    rvol_multiplier     DOUBLE PRECISION,
    vwap_z_score        DOUBLE PRECISION,

    -- Latency
    webhook_latency_ms  INTEGER,

    -- Triple-barrier labels (filled later by labeler)
    tb_label            INTEGER,            -- +1=WIN, -1=LOSS, 0=TIMEOUT
    tb_return           DOUBLE PRECISION,
    tb_barrier_hit      VARCHAR(10),        -- TP / SL / TIMEOUT
    tb_hold_bars        INTEGER,
    tb_hold_minutes     DOUBLE PRECISION,
    tb_max_favorable    DOUBLE PRECISION,   -- MFE pips
    tb_max_adverse      DOUBLE PRECISION,   -- MAE pips
    tb_labeled_at       TIMESTAMPTZ,

    -- Meta-label
    meta_label          BOOLEAN,            -- TRUE = profitable

    -- Raw data
    raw_payload         JSONB,
    enrichment_data     JSONB,
    feature_vector      JSONB,
    desks_matched       JSONB
);

-- Indexes for shadow_signals
CREATE INDEX IF NOT EXISTS ix_shadow_signals_symbol_created
    ON shadow_signals (symbol, created_at);

CREATE INDEX IF NOT EXISTS ix_shadow_signals_desk_created
    ON shadow_signals (desk_id, created_at);

CREATE INDEX IF NOT EXISTS ix_shadow_signals_tb_label_null
    ON shadow_signals (tb_label)
    WHERE tb_label IS NULL;

CREATE INDEX IF NOT EXISTS ix_shadow_signals_tb_label_desk
    ON shadow_signals (tb_label, desk_id)
    WHERE tb_label IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_shadow_signals_feature_vector
    ON shadow_signals USING GIN (feature_vector);


-- ──────────────────────────────────────────────────────────────
-- 2. sim_profiles — simulation account configurations
-- ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS sim_profiles (
    id                  SERIAL PRIMARY KEY,
    name                VARCHAR(30) UNIQUE NOT NULL,
    initial_balance     DOUBLE PRECISION NOT NULL DEFAULT 100000,
    current_balance     DOUBLE PRECISION NOT NULL DEFAULT 100000,
    leverage            INTEGER      NOT NULL DEFAULT 100,
    risk_pct            DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    max_daily_loss_pct  DOUBLE PRECISION NOT NULL DEFAULT 5.0,
    max_total_loss_pct  DOUBLE PRECISION NOT NULL DEFAULT 10.0,
    trailing_drawdown   BOOLEAN NOT NULL DEFAULT FALSE,
    is_active           BOOLEAN NOT NULL DEFAULT TRUE
);

-- Default profiles
INSERT INTO sim_profiles (name, initial_balance, current_balance, leverage, risk_pct)
VALUES
    ('SRV_100', 100000, 100000, 100, 1.0),
    ('SRV_30',  100000, 100000, 30,  0.75),
    ('MT5_1M',  1000000, 1000000, 100, 1.25)
ON CONFLICT (name) DO NOTHING;


-- ──────────────────────────────────────────────────────────────
-- 3. sim_orders — order submission with execution simulation
-- ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS sim_orders (
    id                  SERIAL PRIMARY KEY,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Relationships
    profile_id          INTEGER NOT NULL REFERENCES sim_profiles(id),
    shadow_signal_id    INTEGER REFERENCES shadow_signals(id),
    signal_id           INTEGER,

    -- Identity
    symbol              VARCHAR(20)  NOT NULL,
    direction           VARCHAR(10)  NOT NULL,
    desk_id             VARCHAR(30),
    order_type          VARCHAR(20)  NOT NULL DEFAULT 'MARKET',

    -- Requested
    requested_price     DOUBLE PRECISION,
    requested_qty       DOUBLE PRECISION,
    requested_sl        DOUBLE PRECISION,
    requested_tp1       DOUBLE PRECISION,
    requested_tp2       DOUBLE PRECISION,

    -- Execution simulation
    fill_price          DOUBLE PRECISION,
    fill_qty            DOUBLE PRECISION,
    spread_cost         DOUBLE PRECISION,
    slippage_cost       DOUBLE PRECISION,
    commission          DOUBLE PRECISION,
    simulated_latency_ms INTEGER,

    -- Sizing
    lot_size            DOUBLE PRECISION,
    risk_pct            DOUBLE PRECISION,
    risk_dollars        DOUBLE PRECISION,
    position_size_modifier DOUBLE PRECISION DEFAULT 1.0,

    -- Status
    status              VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    reject_reason       VARCHAR(200),
    filled_at           TIMESTAMPTZ
);


-- ──────────────────────────────────────────────────────────────
-- 4. sim_positions — open/closed position tracking with P&L
-- ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS sim_positions (
    id                  SERIAL PRIMARY KEY,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Relationships
    profile_id          INTEGER NOT NULL REFERENCES sim_profiles(id),
    order_id            INTEGER REFERENCES sim_orders(id),
    shadow_signal_id    INTEGER,
    symbol              VARCHAR(20) NOT NULL,
    direction           VARCHAR(10) NOT NULL,
    desk_id             VARCHAR(30),

    -- Entry
    entry_price         DOUBLE PRECISION,
    entry_time          TIMESTAMPTZ,
    lot_size            DOUBLE PRECISION,

    -- Levels
    stop_loss           DOUBLE PRECISION,
    take_profit_1       DOUBLE PRECISION,
    take_profit_2       DOUBLE PRECISION,
    trailing_stop       DOUBLE PRECISION,
    breakeven_price     DOUBLE PRECISION,

    -- Live tracking
    current_price       DOUBLE PRECISION,
    unrealized_pnl      DOUBLE PRECISION,
    max_favorable_pips  DOUBLE PRECISION,   -- MFE
    max_adverse_pips    DOUBLE PRECISION,   -- MAE

    -- Partial close
    partial_close_pct   DOUBLE PRECISION,
    partial_pnl         DOUBLE PRECISION,

    -- Exit
    exit_price          DOUBLE PRECISION,
    exit_time           TIMESTAMPTZ,
    exit_reason         VARCHAR(30),         -- SL_HIT / TP1_HIT / TP2_HIT / TRAILING / TIME_EXIT / SESSION_CLOSE

    -- Result
    realized_pnl_pips   DOUBLE PRECISION,
    realized_pnl_dollars DOUBLE PRECISION,
    hold_time_minutes   DOUBLE PRECISION,
    commission_total    DOUBLE PRECISION,
    swap_total          DOUBLE PRECISION,
    net_pnl             DOUBLE PRECISION,

    -- Status
    status              VARCHAR(20) NOT NULL DEFAULT 'OPEN',
    risk_flags          JSONB
);


-- ──────────────────────────────────────────────────────────────
-- 5. sim_equity_snapshots — drawdown tracking
-- ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS sim_equity_snapshots (
    id                  SERIAL PRIMARY KEY,
    profile_id          INTEGER NOT NULL REFERENCES sim_profiles(id),
    snapshot_time       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    equity              DOUBLE PRECISION,
    balance             DOUBLE PRECISION,
    unrealized_pnl      DOUBLE PRECISION,
    daily_pnl           DOUBLE PRECISION,
    daily_high          DOUBLE PRECISION,
    high_water_mark     DOUBLE PRECISION,
    daily_drawdown_pct  DOUBLE PRECISION,
    total_drawdown_pct  DOUBLE PRECISION,
    open_positions      INTEGER,
    snapshot_date       DATE
);


-- ──────────────────────────────────────────────────────────────
-- 6. spread_reference — realistic spread modeling per symbol
-- ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS spread_reference (
    symbol              VARCHAR(20) PRIMARY KEY,
    asset_class         VARCHAR(20) NOT NULL,
    base_spread_pips    DOUBLE PRECISION NOT NULL,
    asian_mult          DOUBLE PRECISION NOT NULL DEFAULT 1.8,
    london_mult         DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    ny_mult             DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    overlap_mult        DOUBLE PRECISION NOT NULL DEFAULT 0.8,
    post_ny_mult        DOUBLE PRECISION NOT NULL DEFAULT 1.5,
    commission_per_lot  DOUBLE PRECISION NOT NULL DEFAULT 0
);

-- Seed spread data for all 42 symbols
INSERT INTO spread_reference (symbol, asset_class, base_spread_pips, commission_per_lot) VALUES
    -- Forex Majors (1.0-1.8 pips)
    ('EURUSD', 'forex', 1.0, 0),
    ('GBPUSD', 'forex', 1.2, 0),
    ('USDJPY', 'forex', 1.0, 0),
    ('USDCHF', 'forex', 1.3, 0),
    ('AUDUSD', 'forex', 1.2, 0),
    ('USDCAD', 'forex', 1.4, 0),
    ('NZDUSD', 'forex', 1.5, 0),
    ('EURGBP', 'forex', 1.3, 0),
    -- Forex Crosses (1.5-4.0 pips)
    ('EURJPY', 'forex_cross', 1.5, 0),
    ('GBPJPY', 'forex_cross', 2.0, 0),
    ('AUDJPY', 'forex_cross', 1.8, 0),
    ('NZDJPY', 'forex_cross', 2.2, 0),
    ('EURAUD', 'forex_cross', 2.0, 0),
    ('EURNZD', 'forex_cross', 2.5, 0),
    ('GBPAUD', 'forex_cross', 2.5, 0),
    ('GBPNZD', 'forex_cross', 3.0, 0),
    ('GBPCAD', 'forex_cross', 2.5, 0),
    ('EURCAD', 'forex_cross', 2.0, 0),
    ('AUDCAD', 'forex_cross', 2.0, 0),
    ('AUDNZD', 'forex_cross', 2.0, 0),
    ('CADCHF', 'forex_cross', 2.2, 0),
    ('CADJPY', 'forex_cross', 2.0, 0),
    ('CHFJPY', 'forex_cross', 2.2, 0),
    ('NZDCAD', 'forex_cross', 2.5, 0),
    ('EURCHF', 'forex_cross', 1.8, 0),
    ('GBPCHF', 'forex_cross', 2.5, 0),
    ('AUDCHF', 'forex_cross', 2.2, 0),
    ('NZDCHF', 'forex_cross', 2.8, 0),
    -- Commodities
    ('XAUUSD', 'commodity', 3.0, 0),
    ('XAGUSD', 'commodity', 3.0, 0),
    ('WTIUSD', 'commodity', 3.0, 0),
    -- Indices (1.5-2.0 pips)
    ('US30',   'index', 2.0, 0),
    ('US500',  'index', 0.5, 0),
    ('US100',  'index', 1.5, 0),
    ('GER40',  'index', 1.5, 0),
    ('UK100',  'index', 1.8, 0),
    ('JPN225', 'index', 2.0, 0),
    -- Crypto (variable)
    ('BTCUSD', 'crypto', 30.0, 0),
    ('ETHUSD', 'crypto', 1.5, 0),
    ('SOLUSD', 'crypto', 0.05, 0),
    -- Equities (0.02-0.05)
    ('AAPL',   'equity', 0.02, 1.0),
    ('TSLA',   'equity', 0.05, 1.0)
ON CONFLICT (symbol) DO NOTHING;


-- ──────────────────────────────────────────────────────────────
-- 7. ohlcv_1m — 1-minute OHLCV data for triple-barrier labeling
--    and backtesting
-- ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ohlcv_1m (
    time            TIMESTAMPTZ NOT NULL,
    symbol          VARCHAR(20) NOT NULL,
    open            DOUBLE PRECISION,
    high            DOUBLE PRECISION,
    low             DOUBLE PRECISION,
    close           DOUBLE PRECISION,
    volume          DOUBLE PRECISION,
    PRIMARY KEY (time, symbol)
);

CREATE INDEX IF NOT EXISTS ix_ohlcv_1m_symbol_time
    ON ohlcv_1m (symbol, time);
