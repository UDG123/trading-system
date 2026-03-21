"""
TelegramService — Dual-Routing Broadcaster
Every STRIKE is broadcast to:
  1. TG_PORTFOLIO (master channel — all signals)
  2. TG_DESK{N} (specific desk channel — only that desk's signals)
"""
import logging

import httpx

from app.config import (
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_DESK_CHANNELS,
    TELEGRAM_PORTFOLIO_CHAT,
)

logger = logging.getLogger("OniQuant.Telegram")


class TelegramService:
    """Dual-route broadcaster: Portfolio + Desk-specific channels."""

    def __init__(self):
        self.token = TELEGRAM_BOT_TOKEN
        self.portfolio_id = TELEGRAM_PORTFOLIO_CHAT
        self.desk_channels = TELEGRAM_DESK_CHANNELS
        self.url = f"https://api.telegram.org/bot{self.token}/sendMessage"

    async def broadcast_signal(self, desk_id: str, message: str):
        """
        Routes the signal to the Master Portfolio AND the specific Desk channel.

        Args:
            desk_id: e.g. "DESK4_GOLD" — maps to TELEGRAM_DESK_CHANNELS
            message: Pre-formatted message from OracleBridge.format_strike_message()
        """
        # Resolve desk channel from config (not raw env vars)
        desk_tg_id = self.desk_channels.get(desk_id)

        # Dual-route targets: Portfolio always, Desk if mapped
        targets = []
        if self.portfolio_id:
            targets.append(("PORTFOLIO", self.portfolio_id))
        if desk_tg_id:
            targets.append((desk_id, desk_tg_id))

        if not targets:
            logger.warning(f"No Telegram targets for desk_id={desk_id}")
            return

        async with httpx.AsyncClient(timeout=10.0) as client:
            for label, chat_id in targets:
                try:
                    resp = await client.post(self.url, json={
                        "chat_id": chat_id,
                        "text": message,
                        "parse_mode": "Markdown",
                        "disable_web_page_preview": False,
                    })
                    if resp.status_code == 200:
                        logger.info(f"Broadcast → {label} ({chat_id}) OK")
                    else:
                        logger.warning(
                            f"Broadcast → {label} ({chat_id}) HTTP {resp.status_code}: "
                            f"{resp.text[:200]}"
                        )
                except Exception as e:
                    logger.error(f"Broadcast → {label} ({chat_id}) FAILED: {e}")

    async def send_to_portfolio(self, message: str):
        """Send a message to the Portfolio channel only."""
        if not self.portfolio_id:
            return
        async with httpx.AsyncClient(timeout=10.0) as client:
            try:
                await client.post(self.url, json={
                    "chat_id": self.portfolio_id,
                    "text": message,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True,
                })
            except Exception as e:
                logger.error(f"Portfolio send failed: {e}")
