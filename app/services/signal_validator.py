"""
Signal Validator - Validates incoming LuxAlgo alerts.
Checks: alert type validity, desk routing, session windows,
required fields, and basic sanity checks.
"""
import logging
from datetime import datetime, timezone
from typing import List, Tuple, Optional

from app.schemas import TradingViewAlert
from app.config import VALID_ALERT_TYPES, DESKS

logger = logging.getLogger("TradingSystem.Validator")


class SignalValidator:
    """Validates incoming signals before they enter the pipeline."""

    def validate(
        self,
        alert: TradingViewAlert,
        normalized_symbol: str,
        matched_desks: List[str],
    ) -> Tuple[bool, Optional[List[str]]]:
        """
        Run all validation checks. Returns (is_valid, errors).
        """
        errors: List[str] = []

        # 1. Alert type must be recognized
        if alert.alert_type not in VALID_ALERT_TYPES:
            errors.append(
                f"Unknown alert_type '{alert.alert_type}'. "
                f"Valid: {VALID_ALERT_TYPES}"
            )

        # 2. Must route to at least one desk
        if not matched_desks:
            errors.append(
                f"Symbol '{normalized_symbol}' does not match any desk"
            )

        # 3. Price should be positive (pipeline fetches live price as backup)
        if alert.price <= 0 and not matched_desks:
            errors.append(f"Invalid price: {alert.price} and no desk matched")

        # 4. For entry signals, we need at least SL1
        entry_types = {
            "bullish_confirmation", "bearish_confirmation",
            "bullish_plus", "bearish_plus",
            "bullish_confirmation_plus", "bearish_confirmation_plus",
            "contrarian_bullish", "contrarian_bearish",
        }
        # 4. For entry signals, warn if missing SL (pipeline will calculate from ATR)
        if alert.alert_type in entry_types and alert.sl1 is None:
            logger.warning(
                f"Entry signal '{alert.alert_type}' missing sl1 — pipeline will calculate from ATR"
            )

        # 5. SL/TP sanity: SL must be on correct side of price
        if alert.sl1 is not None:
            if "bullish" in alert.alert_type and alert.sl1 >= alert.price:
                errors.append(
                    f"Bullish signal but SL1 ({alert.sl1}) >= price ({alert.price})"
                )
            if "bearish" in alert.alert_type and alert.sl1 <= alert.price:
                errors.append(
                    f"Bearish signal but SL1 ({alert.sl1}) <= price ({alert.price})"
                )

        # 6. TP sanity: TP must be on correct side of price
        if alert.tp1 is not None:
            if "bullish" in alert.alert_type and alert.tp1 <= alert.price:
                errors.append(
                    f"Bullish signal but TP1 ({alert.tp1}) <= price ({alert.price})"
                )
            if "bearish" in alert.alert_type and alert.tp1 >= alert.price:
                errors.append(
                    f"Bearish signal but TP1 ({alert.tp1}) >= price ({alert.price})"
                )

        # 7. Filter desks that don't allow this alert type
        filtered_desks = []
        for desk_id in matched_desks:
            desk = DESKS.get(desk_id)
            if desk and desk.get("alerts") != "ALL":
                allowed = desk.get("alerts", [])
                if alert.alert_type not in allowed:
                    logger.debug(
                        f"Alert '{alert.alert_type}' not in {desk_id} allowed list — skipping desk"
                    )
                    continue
            filtered_desks.append(desk_id)
        matched_desks = filtered_desks

        if not matched_desks:
            errors.append(
                f"Alert '{alert.alert_type}' not allowed by any matched desk"
            )

        # 8. Timeframe sanity (basic - full session check is Phase 2)
        if not alert.timeframe:
            errors.append("Missing timeframe")

        is_valid = len(errors) == 0

        if errors:
            logger.debug(
                f"Validation failed for {normalized_symbol} "
                f"{alert.alert_type}: {errors}"
            )

        return is_valid, errors if errors else None
