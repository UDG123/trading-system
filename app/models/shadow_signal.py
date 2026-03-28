"""
ShadowSignal Model — logs EVERY incoming signal with full feature context
regardless of whether gates would block it. Primary table for ML data
collection and triple-barrier labeling.
"""
from datetime import datetime, timezone
from sqlalchemy import (
    Column, Integer, String, Float, DateTime, Text, Boolean, Index,
)
from sqlalchemy.dialects.postgresql import JSONB
from app.database import Base


class ShadowSignal(Base):
    __tablename__ = "shadow_signals"

    id = Column(Integer, primary_key=True, autoincrement=True)
    created_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    # ── Signal identity ──
    symbol = Column(String(20), nullable=False)
    direction = Column(String(10))
    alert_type = Column(String(50))
    timeframe = Column(String(10))
    desk_id = Column(String(30))
    price = Column(Float)

    # ── LuxAlgo levels ──
    sl1 = Column(Float)
    tp1 = Column(Float)
    tp2 = Column(Float)
    smart_trail = Column(Float)

    # ── Gate values logged as FEATURES ──
    hurst_exponent = Column(Float)
    indicator_alignment = Column(Float)         # 0.0-1.0
    consensus_score = Column(Integer)
    consensus_tier = Column(String(10))
    ml_score = Column(Float)
    claude_decision = Column(String(20))
    claude_confidence = Column(Float)
    claude_reasoning = Column(Text)

    # ── "Would block" flags ──
    hurst_would_block = Column(Boolean)
    alignment_would_block = Column(Boolean)
    consensus_would_block = Column(Boolean)
    cto_would_skip = Column(Boolean)
    live_pipeline_approved = Column(Boolean)

    # ── Market context ──
    rsi = Column(Float)
    adx = Column(Float)
    atr = Column(Float)
    atr_pct = Column(Float)
    ema50 = Column(Float)
    ema200 = Column(Float)
    trend = Column(String(20))
    volatility_regime = Column(String(20))
    active_session = Column(String(30))
    kill_zone_type = Column(String(20))
    vix_level = Column(Float)
    dxy_change_pct = Column(Float)

    # ── Cross-asset ──
    correlated_open_count = Column(Integer)
    correlation_group = Column(String(30))

    # ── Volume ──
    volume = Column(Float)
    rvol_multiplier = Column(Float)
    vwap_z_score = Column(Float)

    # ── Latency ──
    webhook_latency_ms = Column(Integer)

    # ── Triple-barrier labels (filled later by labeler) ──
    tb_label = Column(Integer)              # +1=WIN, -1=LOSS, 0=TIMEOUT
    tb_return = Column(Float)
    tb_barrier_hit = Column(String(10))     # TP / SL / TIMEOUT
    tb_hold_bars = Column(Integer)
    tb_hold_minutes = Column(Float)
    tb_max_favorable = Column(Float)        # MFE pips
    tb_max_adverse = Column(Float)          # MAE pips
    tb_labeled_at = Column(DateTime(timezone=True))

    # ── Meta-label ──
    meta_label = Column(Boolean)            # TRUE = profitable

    # ── HMM Regime Detection ──
    hmm_regime = Column(String(20))
    hmm_confidence = Column(Float)
    hmm_state_probs = Column(JSONB)

    # ── Meta-labeling ──
    meta_probability = Column(Float)
    meta_should_trade = Column(Boolean)
    meta_bet_size = Column(Float)

    # ── Volatility Targeting ──
    vol_multiplier = Column(Float)
    realized_vol = Column(Float)
    vol_method = Column(String(20))

    # ── HAR-RV ──
    har_rv_forecast = Column(Float)
    har_rv_r_squared = Column(Float)

    # ── Raw data ──
    raw_payload = Column(JSONB)
    enrichment_data = Column(JSONB)
    feature_vector = Column(JSONB)
    desks_matched = Column(JSONB)

    __table_args__ = (
        Index("ix_shadow_signals_symbol_created", "symbol", "created_at"),
        Index("ix_shadow_signals_desk_created", "desk_id", "created_at"),
        Index(
            "ix_shadow_signals_tb_label_null", "tb_label",
            postgresql_where=Column("tb_label").is_(None),
        ),
        Index(
            "ix_shadow_signals_tb_label_desk", "tb_label", "desk_id",
            postgresql_where=Column("tb_label").isnot(None),
        ),
    )

    def __repr__(self):
        return (
            f"<ShadowSignal {self.id} | {self.symbol} {self.direction} | "
            f"approved={self.live_pipeline_approved}>"
        )
