import json
import redis
from app.config import settings

STREAMS = {
    'raw': 'oniquant:signals:raw',
    'validated': 'oniquant:signals:validated',
    'rejected': 'oniquant:signals:rejected',
    'orders_paper': 'oniquant:orders:paper',
    'orders_filled': 'oniquant:orders:filled',
    'risk_events': 'oniquant:risk:events',
    'system_events': 'oniquant:system:events',
}


def get_redis() -> redis.Redis:
    return redis.Redis.from_url(settings.redis_url, decode_responses=True)


def publish_event(stream: str, event: dict) -> str:
    try:
        return get_redis().xadd(stream, {'payload': json.dumps(event, default=str)})
    except Exception:
        return 'offline'


def consume_group(stream: str, group: str, consumer: str, count: int = 10, block: int = 1000):
    r = get_redis()
    try:
        r.xgroup_create(stream, group, id='0', mkstream=True)
    except redis.ResponseError:
        pass
    return r.xreadgroup(group, consumer, {stream: '>'}, count=count, block=block)


def ack_event(stream: str, group: str, message_id: str):
    return get_redis().xack(stream, group, message_id)


async def publish_signal(redis_client, payload: dict) -> str:
    """Publish an internal-engine signal using the worker-compatible stream format."""
    body = json.dumps(payload, default=str)
    return await redis_client.xadd("oniquant_alerts", {"payload": body})
