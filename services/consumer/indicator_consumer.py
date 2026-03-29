import json
import os
import asyncio
from typing import Protocol
import aiokafka
import redis.asyncio as redis
from Indicators import RunningSMA, RunningRSI
from services.consumer.dead_letter_queue import publish_to_dlq
from utils import retry_policy


class Indicator(Protocol):
    def add(self, price: float) -> float | None: ...


class IndicatorEngineConsumer:
    def __init__(self):
        self.indicator_templates: dict[str, type] = {}
        self.indicator_kwargs: dict[str, dict] = {}
        self.product_indicators: dict[str, dict[str, Indicator]] = {}
        self.redis = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379"))
        self.consumer = aiokafka.AIOKafkaConsumer(
            "market_trades",
            bootstrap_servers="localhost:9092",
            group_id="indicator_engine",
            value_deserializer=lambda x: json.loads(x) if x else None,
            auto_offset_reset="earliest",
        )
        self.dlq_producer = aiokafka.AIOKafkaProducer(bootstrap_servers="localhost:9092")

    def add_indicator(self, name: str, indicator_cls: type, **kwargs):
        self.indicator_templates[name] = indicator_cls
        self.indicator_kwargs[name] = kwargs

    def get_indicators(self, product_id: str) -> dict[str, Indicator]:
        if product_id not in self.product_indicators:
            self.product_indicators[product_id] = {
                name: cls(**self.indicator_kwargs[name])
                for name, cls in self.indicator_templates.items()
            }
        return self.product_indicators[product_id]

    @retry_policy
    async def publish_to_redis(self, product_id: str, indicators: dict):
        async with self.redis.pipeline() as pipe:
            for name, value in indicators.items():
                if value is not None:
                    pipe.set(f"indicator:{product_id}:{name}", value)
            await pipe.execute()

    @retry_policy
    async def run(self):
        await self.consumer.start()
        await self.dlq_producer.start()
        try:
            async for message in self.consumer:
                try:
                    if message.value is None:
                        continue
                    for event in message.value.get("events", []):
                        for ticker in event.get("tickers", []):
                            price = float(ticker["price"])
                            product_id = ticker["product_id"]
                            indicators = self.get_indicators(product_id)
                            results = {
                                name: ind.add(price) for name, ind in indicators.items()
                            }
                            await self.publish_to_redis(product_id, results)
                            print(f"{product_id} | Price: {price} | {results}")
                except Exception as e:
                    await publish_to_dlq(self.dlq_producer, message, e)
        finally:
            await self.consumer.stop()
            await self.dlq_producer.stop()
            await self.redis.aclose()


if __name__ == "__main__":
    engine = IndicatorEngineConsumer()
    asyncio.run(engine.run())
