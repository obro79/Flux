import asyncio
import os

from base_consumer import BaseConsumer
from metrics import redis_writes_total
from models import Trade


class RawConsumer(BaseConsumer):
    def __init__(self):
        super().__init__(group_id="raw_consumer", redis_url=os.getenv("REDIS_URL", "redis://localhost:6379"))

    async def process_trade(self, trade: Trade) -> None:
        product_key = f"{trade.exchange}:{trade.product_id}"
        async with self.redis.pipeline() as pipe:
            pipe.set(f"crypto:{product_key}:price", trade.price)
            pipe.set(f"crypto:{product_key}:last_size", trade.size)
            await pipe.execute()
        redis_writes_total.labels(
            consumer_group=self.group_id,
            exchange=trade.exchange,
            keyspace="crypto",
        ).inc(2)


if __name__ == "__main__":
    consumer = RawConsumer()
    asyncio.run(consumer.run())
