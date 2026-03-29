import json
import logging
import os
import asyncio
from typing import Protocol
import aiokafka
import redis.asyncio as redis
from Indicators import RunningSMA, RunningRSI
from dead_letter_queue import publish_to_dlq
from models import MarketTradeMessage
from utils import retry_policy
from metrics import messages_consumed_total, redis_writes_total


logger = logging.getLogger(__name__)


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
        self.dlq_producer = aiokafka.AIOKafkaProducer(
            bootstrap_servers="localhost:9092"
        )

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
                    redis_writes_total.inc()
            await pipe.execute()

    @retry_policy
    async def run(self):
        await self.consumer.start()
        await self.dlq_producer.start()
        try:
            async for message in self.consumer:
                try:
                    messages_consumed_total.inc()
                    if message.value is None:
                        continue
                    msg = MarketTradeMessage(**message.value)
                    for event in msg.events:
                        for ticker in event.tickers:
                            indicators = self.get_indicators(ticker.product_id)
                            results = {
                                name: ind.add(ticker.price)
                                for name, ind in indicators.items()
                            }
                            await self.publish_to_redis(ticker.product_id, results)
                            logger.info(
                                "Indicators updated",
                                extra={
                                    "product_id": ticker.product_id,
                                    "price": ticker.price,
                                    "indicators": results,
                                },
                            )
                except Exception as e:
                    await publish_to_dlq(self.dlq_producer, message, e)
                    dlq_messages_total.inc()
        finally:
            await self.consumer.stop()
            await self.dlq_producer.stop()
            await self.redis.aclose()


if __name__ == "__main__":
    engine = IndicatorEngineConsumer()
    asyncio.run(engine.run())
