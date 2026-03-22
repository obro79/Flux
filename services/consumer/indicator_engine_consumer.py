import json
import os
import asyncio
from typing import Protocol
import aiokafka
import redis.asyncio as redis
from Indicators import RunningSMA, RunningRSI


class Indicator(Protocol):
    def add(self, price: float) -> float | None: ...


class IndicatorEngineConsumer:
    def __init__(self):
        self.indicators: dict[str, Indicator] = {}
        self.redis = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379"))
        self.consumer = aiokafka.AIOKafkaConsumer(
            "market_trades",
            bootstrap_servers="localhost:9092",
            group_id="market_trades",
            value_deserializer=lambda x: json.loads(x) if x else None,
            auto_offset_reset="earliest",
        )

    def add_indicator(self, name: str, indicator):
        self.indicators[name] = indicator

    def add_price(self, price: float) -> dict:
        return {name: ind.add(price) for name, ind in self.indicators.items()}

    async def publish_to_redis(self, indicators: dict):
        async with self.redis.pipeline() as pipe:
            for name, value in indicators.items():
                if value is not None:
                    pipe.set(f"indicator:{name}", value)
            await pipe.execute()

    async def run(self):
        await self.consumer.start()
        try:
            async for message in self.consumer:
                for event in message.value.get("events", []):
                    for ticker in event.get("tickers", []):
                        price = float(ticker["price"])
                        indicators = self.add_price(price)
                        await self.publish_to_redis(indicators)
                        print(f"Price: {price} | {indicators}")
        finally:
            await self.consumer.stop()
            await self.redis.aclose()


if __name__ == "__main__":
    engine = IndicatorEngineConsumer()
    asyncio.run(engine.run())
