-- ============================================================
-- OniQuant → NovaQuant Schema Alignment Migration
-- Task 1: ALTER TABLE commands for ml_trade_logs + signals
-- ============================================================

-- 1a. Add webhook_latency_ms to ml_trade_logs
ALTER TABLE ml_trade_logs
ADD COLUMN IF NOT EXISTS webhook_latency_ms INTEGER DEFAULT NULL;

-- 1b. Add rationale JSONB column (NovaQuant schema — structured CTO reasoning)
ALTER TABLE ml_trade_logs
ADD COLUMN IF NOT EXISTS rationale JSONB DEFAULT '{}' NOT NULL;

-- 1c. Ensure status on trades table can accept VIRTUAL_OPEN
-- (The column is VARCHAR(20), VIRTUAL_OPEN is 12 chars — fits.
--  But let's widen to 30 to future-proof for all statuses.)
ALTER TABLE trades
ALTER COLUMN status TYPE VARCHAR(30);

-- 1d. Add webhook_latency_ms to signals table too (source of truth)
ALTER TABLE signals
ADD COLUMN IF NOT EXISTS webhook_latency_ms INTEGER DEFAULT NULL;

-- 1e. Add index for latency analysis
CREATE INDEX IF NOT EXISTS ix_signals_latency
ON signals (webhook_latency_ms)
WHERE webhook_latency_ms IS NOT NULL;

-- 1f. Add index for virtual trade queries
CREATE INDEX IF NOT EXISTS ix_trades_virtual
ON trades (status)
WHERE status IN ('VIRTUAL_OPEN', 'ONIAI_OPEN', 'EXECUTED', 'OPEN');

-- Verify
-- SELECT column_name, data_type, column_default
-- FROM information_schema.columns
-- WHERE table_name = 'ml_trade_logs' AND column_name IN ('webhook_latency_ms', 'rationale');
