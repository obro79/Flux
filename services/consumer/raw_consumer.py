import json
import os
import asyncio
import aiokafka
import redis.asyncio as redis
from .dead_letter_queue import publish_to_dlq
from .models import MarketTradeMessage
from utils import retry_policy


class RawConsumer:
    def __init__(self):
        self.redis = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379"))
        self.consumer = aiokafka.AIOKafkaConsumer(
            "market_trades",
            bootstrap_servers="localhost:9092",
            group_id="raw_consumer",
            value_deserializer=lambda x: json.loads(x) if x else None,
            auto_offset_reset="earliest",
        )
        self.dlq_producer = aiokafka.AIOKafkaProducer(bootstrap_servers="localhost:9092")

    @retry_policy
    async def run(self):
        await self.consumer.start()
        await self.dlq_producer.start()
        try:
            async for message in self.consumer:
                try:
                    if message.value is None:
                        continue
                    msg = MarketTradeMessage(**message.value)
                    for event in msg.events:
                        for ticker in event.tickers:
                            async with self.redis.pipeline() as pipe:
                                pipe.set(f"crypto:{ticker.product_id}:price", ticker.price)
                                pipe.set(f"crypto:{ticker.product_id}:volume_24h", ticker.volume_24_h)
                                await pipe.execute()
                except Exception as e:
                    await publish_to_dlq(self.dlq_producer, message, e)
        finally:
            await self.consumer.stop()
            await self.dlq_producer.stop()
            await self.redis.aclose()


if __name__ == "__main__":
    consumer = RawConsumer()
    asyncio.run(consumer.run())
