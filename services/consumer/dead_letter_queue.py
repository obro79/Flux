import json
from datetime import datetime, timezone


async def publish_to_dlq(producer, message, error, *, exchange: str):
    payload = {
        "exchange": exchange,
        "original_topic": message.topic,
        "original_offset": message.offset,
        "original_value": message.value,
        "error": str(error),
        "error_type": type(error).__name__,
        "failed_at": datetime.now(timezone.utc).isoformat(),
    }
    await producer.send("market_trades.dlq", json.dumps(payload).encode())
