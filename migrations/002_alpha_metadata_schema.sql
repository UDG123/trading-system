-- ============================================================
-- Alpha Metadata Schema Migration
-- Adds precision metrics to ml_trade_logs and pending memory
-- engine support to signals.
-- All new columns are NULLable to preserve historical records.
-- ============================================================

-- 2a. Add Hurst Exponent to ml_trade_logs (trending vs choppy: 0-1)
ALTER TABLE ml_trade_logs
ADD COLUMN IF NOT EXISTS hurst_exponent FLOAT DEFAULT NULL;

-- 2b. Add Relative Volume multiplier at signal time
ALTER TABLE ml_trade_logs
ADD COLUMN IF NOT EXISTS rvol_multiplier FLOAT DEFAULT NULL;

-- 2c. Add VWAP Z-Score (std deviations from VWAP, primarily for equities)
ALTER TABLE ml_trade_logs
ADD COLUMN IF NOT EXISTS vwap_z_score FLOAT DEFAULT NULL;

-- 2d. Add pending wait time (minutes parked in memory before fill)
ALTER TABLE ml_trade_logs
ADD COLUMN IF NOT EXISTS pending_wait_time_mins FLOAT DEFAULT NULL;

-- 2e. Widen signals.status to 30 chars to support PENDING_CONDITIONS
ALTER TABLE signals
ALTER COLUMN status TYPE VARCHAR(30);

-- 2f. Add TTL expiry for Pending Memory Engine
ALTER TABLE signals
ADD COLUMN IF NOT EXISTS ttl_expiry TIMESTAMPTZ DEFAULT NULL;

-- 2g. Index for pending signals lookup
CREATE INDEX IF NOT EXISTS ix_signals_pending
ON signals (status, ttl_expiry)
WHERE status = 'PENDING_CONDITIONS';

-- Verify
-- SELECT column_name, data_type, is_nullable
-- FROM information_schema.columns
-- WHERE table_name = 'ml_trade_logs'
--   AND column_name IN ('hurst_exponent', 'rvol_multiplier', 'vwap_z_score', 'pending_wait_time_mins');
--
-- SELECT column_name, data_type, is_nullable
-- FROM information_schema.columns
-- WHERE table_name = 'signals'
--   AND column_name = 'ttl_expiry';
