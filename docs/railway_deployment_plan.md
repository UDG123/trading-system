# Railway Deployment Plan (Updated Trading System)

1. **Set environment variables**
   - `DESK_CONFIG_PATH` for desk-level ATR/quality/risk overrides.
   - Existing API keys (`TWELVEDATA_API_KEY`, `POLYGON_API_KEY`, DB/Redis URLs).
   - Feature toggles: `ENABLE_HMM_REGIME`, `ENABLE_META_LABELER`, `ENABLE_MEAN_REVERSION`.

2. **Desk configuration rollout**
   - Store desk tuning JSON in Railway volume or mounted config file.
   - Include per-desk keys: `risk_pct`, `atr_multipliers`, `quality_thresholds`.

3. **Model and backtest jobs**
   - Cron task: nightly meta-labeler retrain (`/train_meta` route or worker command).
   - Cron task: daily backtest and digest generation.

4. **Pipeline deployment order**
   - Deploy app with feature flags OFF.
   - Enable HMM probabilities and mean-reversion enhancements.
   - Enable meta-labeler macro features after first successful retrain.

5. **Monitoring dashboard**
   - Export Railway logs to CSV (or ingestion sink).
   - Run `app/services/monitoring_dashboard.py` to generate HTML metrics dashboard.
   - Publish dashboard artifact to internal storage or static endpoint.

6. **Log export and debugging**
   - Use Railway log export CLI / dashboard download.
   - Keep `signal_id`, `desk_id`, HMM probabilities, confluence score in structured logs.
   - For incidents: compare skipped-vs-approved by desk and quality threshold snapshots.
