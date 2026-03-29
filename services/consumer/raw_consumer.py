import os
import asyncio

from base_consumer import BaseConsumer
from models import Ticker
from metrics import redis_writes_total

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")


class RawConsumer(BaseConsumer):
    def __init__(self):
        super().__init__(group_id="raw_consumer", redis_url=REDIS_URL)

    async def process_ticker(self, ticker: Ticker) -> None:
        async with self.redis.pipeline() as pipe:
            pipe.set(f"crypto:{ticker.product_id}:price", ticker.price)
            pipe.set(f"crypto:{ticker.product_id}:volume_24h", ticker.volume_24_h)
            await pipe.execute()
        redis_writes_total.inc()


if __name__ == "__main__":
    consumer = RawConsumer()
    asyncio.run(consumer.run())
