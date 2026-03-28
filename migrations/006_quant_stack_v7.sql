-- Migration 006: Quant Stack v7.0 — New columns for HMM, meta-labeling, vol targeting, HAR-RV

-- shadow_signals: regime detection fields
ALTER TABLE shadow_signals ADD COLUMN IF NOT EXISTS hmm_regime VARCHAR(20);
ALTER TABLE shadow_signals ADD COLUMN IF NOT EXISTS hmm_confidence DOUBLE PRECISION;
ALTER TABLE shadow_signals ADD COLUMN IF NOT EXISTS hmm_state_probs JSONB;

-- shadow_signals: meta-labeling fields
ALTER TABLE shadow_signals ADD COLUMN IF NOT EXISTS meta_probability DOUBLE PRECISION;
ALTER TABLE shadow_signals ADD COLUMN IF NOT EXISTS meta_should_trade BOOLEAN;
ALTER TABLE shadow_signals ADD COLUMN IF NOT EXISTS meta_bet_size DOUBLE PRECISION;

-- shadow_signals: volatility targeting fields
ALTER TABLE shadow_signals ADD COLUMN IF NOT EXISTS vol_multiplier DOUBLE PRECISION;
ALTER TABLE shadow_signals ADD COLUMN IF NOT EXISTS realized_vol DOUBLE PRECISION;
ALTER TABLE shadow_signals ADD COLUMN IF NOT EXISTS vol_method VARCHAR(20);

-- shadow_signals: HAR-RV fields
ALTER TABLE shadow_signals ADD COLUMN IF NOT EXISTS har_rv_forecast DOUBLE PRECISION;
ALTER TABLE shadow_signals ADD COLUMN IF NOT EXISTS har_rv_r_squared DOUBLE PRECISION;

-- ml_trade_logs: quant stack fields
ALTER TABLE ml_trade_logs ADD COLUMN IF NOT EXISTS hmm_regime VARCHAR(20);
ALTER TABLE ml_trade_logs ADD COLUMN IF NOT EXISTS meta_probability DOUBLE PRECISION;
ALTER TABLE ml_trade_logs ADD COLUMN IF NOT EXISTS vol_multiplier DOUBLE PRECISION;
ALTER TABLE ml_trade_logs ADD COLUMN IF NOT EXISTS har_rv_used BOOLEAN DEFAULT FALSE;
ALTER TABLE ml_trade_logs ADD COLUMN IF NOT EXISTS orthogonal_weights JSONB;

-- Index for regime-based queries
CREATE INDEX IF NOT EXISTS ix_shadow_regime ON shadow_signals (hmm_regime, created_at);

-- Index for meta-label filtering analysis
CREATE INDEX IF NOT EXISTS ix_shadow_meta ON shadow_signals (meta_should_trade, tb_label)
    WHERE meta_probability IS NOT NULL;
