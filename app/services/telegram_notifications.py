import os
import httpx
import logging

logger = logging.getLogger("OniQuant.Telegram")


class TelegramService:
    def __init__(self):
        self.token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.portfolio_id = os.getenv("TG_PORTFOLIO")
        # Base URL for Telegram API
        self.url = f"https://api.telegram.org/bot{self.token}/sendMessage"

    async def broadcast_signal(self, desk_id: str, message: str):
        """Routes the signal to the Master Portfolio and the specific Desk."""
        # Map the desk_id to the specific Telegram ID from your variables
        desk_tg_id = os.getenv(f"TG_{desk_id.upper()}")

        # Build the list of channels to notify
        targets = [self.portfolio_id]
        if desk_tg_id:
            targets.append(desk_tg_id)

        async with httpx.AsyncClient() as client:
            for chat_id in targets:
                try:
                    await client.post(self.url, json={
                        "chat_id": chat_id,
                        "text": message,
                        "parse_mode": "Markdown",
                        "disable_web_page_preview": False
                    })
                except Exception as e:
                    logger.error(f"Failed to send to {chat_id}: {e}")
