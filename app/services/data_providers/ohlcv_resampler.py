"""
OHLCV Resampler — derives 5m/15m/1H/4H/Daily bars from 1-minute base bars.
Runs periodically to keep higher-timeframe tables populated from free data sources.
"""
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger("TradingSystem.OHLCVResampler")

# Source → target timeframe mappings with aggregation windows
RESAMPLE_CONFIG = {
    "ohlcv_5m":  {"source": "ohlcv_1m", "minutes": 5},
    "ohlcv_15m": {"source": "ohlcv_1m", "minutes": 15},
    "ohlcv_1h":  {"source": "ohlcv_1m", "minutes": 60},
    "ohlcv_4h":  {"source": "ohlcv_1m", "minutes": 240},
    "ohlcv_1d":  {"source": "ohlcv_1m", "minutes": 1440},
}


def resample_ohlcv(
    db: Session,
    symbol: str,
    target_table: str = None,
    hours_back: int = 2,
) -> Dict[str, int]:
    """
    Resample 1-minute bars into higher timeframes for a single symbol.
    Only processes bars from the last `hours_back` hours to stay efficient.
    Returns dict of {target_table: bars_inserted}.
    """
    results = {}
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours_back)
    cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")

    targets = {target_table: RESAMPLE_CONFIG[target_table]} if target_table else RESAMPLE_CONFIG

    for table, cfg in targets.items():
        try:
            minutes = cfg["minutes"]
            source = cfg["source"]

            # PostgreSQL date_trunc + interval-based aggregation
            # For non-standard intervals (5m, 15m, 4h), use epoch floor
            count = db.execute(
                text(f"""
                    INSERT INTO {table} (time, symbol, open, high, low, close, volume)
                    SELECT
                        to_timestamp(
                            floor(extract(epoch from time) / :seconds) * :seconds
                        ) AT TIME ZONE 'UTC' AS bar_time,
                        symbol,
                        (array_agg(open ORDER BY time ASC))[1] AS open,
                        max(high) AS high,
                        min(low) AS low,
                        (array_agg(close ORDER BY time DESC))[1] AS close,
                        sum(volume) AS volume
                    FROM {source}
                    WHERE symbol = :symbol AND time >= :cutoff
                    GROUP BY bar_time, symbol
                    HAVING count(*) > 0
                    ON CONFLICT (time, symbol) DO UPDATE SET
                        high = GREATEST({table}.high, EXCLUDED.high),
                        low = LEAST({table}.low, EXCLUDED.low),
                        close = EXCLUDED.close,
                        volume = EXCLUDED.volume
                """),
                {
                    "symbol": symbol,
                    "cutoff": cutoff_str,
                    "seconds": minutes * 60,
                },
            )
            db.commit()
            results[table] = count.rowcount if count.rowcount else 0

        except Exception as e:
            db.rollback()
            logger.debug(f"Resample failed for {symbol} → {table}: {e}")
            results[table] = 0

    return results


def resample_all_symbols(
    db: Session,
    symbols: list,
    hours_back: int = 2,
) -> Dict[str, int]:
    """Resample all symbols across all timeframes. Returns total bars per table."""
    totals = {t: 0 for t in RESAMPLE_CONFIG}

    for symbol in symbols:
        result = resample_ohlcv(db, symbol, hours_back=hours_back)
        for table, count in result.items():
            totals[table] += count

    total_bars = sum(totals.values())
    if total_bars > 0:
        logger.info(
            f"Resample complete | {len(symbols)} symbols | "
            f"{total_bars} bars | {totals}"
        )

    return totals
