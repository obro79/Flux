import logging
import os
from typing import Protocol

from base_consumer import BaseConsumer
from models import Trade
from utils import retry_policy
from metrics import redis_writes_total

logger = logging.getLogger(__name__)

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")


class Indicator(Protocol):
    def add(self, price: float) -> float | None: ...


class IndicatorEngineConsumer(BaseConsumer):
    def __init__(self):
        super().__init__(group_id="indicator_engine", redis_url=REDIS_URL)
        self.indicator_templates: dict[str, type] = {}
        self.indicator_kwargs: dict[str, dict] = {}
        self.product_indicators: dict[str, dict[str, Indicator]] = {}

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
        writes = 0
        async with self.redis.pipeline() as pipe:
            for name, value in indicators.items():
                if value is not None:
                    pipe.set(f"indicator:{product_id}:{name}", value)
                    writes += 1
            await pipe.execute()
        return writes

    async def process_trade(self, trade: Trade) -> None:
        product_key = f"{trade.exchange}:{trade.product_id}"
        indicators = self.get_indicators(product_key)
        results = {
            name: ind.add(trade.price)
            for name, ind in indicators.items()
        }
        writes = await self.publish_to_redis(product_key, results)
        if writes:
            redis_writes_total.labels(
                consumer_group=self.group_id,
                exchange=trade.exchange,
                keyspace="indicator",
            ).inc(writes)
        logger.info(
            "Indicators updated",
            extra={
                "exchange": trade.exchange,
                "product_id": trade.product_id,
                "price": trade.price,
                "indicators": results,
            },
        )


if __name__ == "__main__":
    import asyncio

    engine = IndicatorEngineConsumer()
    asyncio.run(engine.run())
