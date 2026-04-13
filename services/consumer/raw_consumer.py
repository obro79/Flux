import os
import asyncio

from base_consumer import BaseConsumer
from models import Trade
from metrics import redis_writes_total

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")


class RawConsumer(BaseConsumer):
    def __init__(self):
        super().__init__(group_id="raw_consumer", redis_url=REDIS_URL)

    async def process_trade(self, trade: Trade) -> None:
        async with self.redis.pipeline() as pipe:
            pipe.set(f"crypto:{trade.product_id}:price", trade.price)
            pipe.set(f"crypto:{trade.product_id}:last_size", trade.size)
            await pipe.execute()
        redis_writes_total.inc()


if __name__ == "__main__":
    consumer = RawConsumer()
    asyncio.run(consumer.run())
