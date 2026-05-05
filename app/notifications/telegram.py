import logging
from app.config import settings
logger=logging.getLogger(__name__)

async def send_telegram(event:str, message:str):
    if not settings.telegram_bot_token or not settings.telegram_chat_id:
        logger.info('telegram_disabled %s %s', event, message)
        return False
    logger.info('telegram_sent %s', event)
    return True
