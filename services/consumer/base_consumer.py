import json
import logging
from abc import ABC, abstractmethod

import aiokafka
import redis.asyncio as aioredis

from services.consumer.dead_letter_queue import publish_to_dlq
from services.consumer.models import MarketTradeMessage, Trade
from services.consumer.metrics import messages_consumed_total, dlq_messages_total

logger = logging.getLogger(__name__)


class BaseConsumer(ABC):
    def __init__(
        self,
        group_id: str,
        redis_url: str | None = None,
        bootstrap_servers: str = "localhost:9092",
    ) -> None:
        self.consumer = aiokafka.AIOKafkaConsumer(
            "market_trades",
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            value_deserializer=lambda x: json.loads(x) if x else None,
            auto_offset_reset="earliest",
        )
        self.dlq_producer = aiokafka.AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers
        )
        self.redis = aioredis.from_url(redis_url) if redis_url else None

    @abstractmethod
    async def process_trade(self, trade: Trade) -> None:
        """Handle a single trade update."""

    async def on_start(self) -> None:
        """Hook for subclasses to run setup after Kafka connects."""

    async def on_stop(self) -> None:
        """Hook for subclasses to run cleanup before shutdown."""

    async def run(self) -> None:
        await self.consumer.start()
        await self.dlq_producer.start()
        await self.on_start()
        try:
            async for message in self.consumer:
                messages_consumed_total.inc()
                try:
                    if message.value is None:
                        continue
                    msg = MarketTradeMessage(**message.value)
                    for event in msg.events:
                        for trade in event.trades:
                            await self.process_trade(trade)
                except Exception as exc:
                    await publish_to_dlq(self.dlq_producer, message, exc)
                    dlq_messages_total.inc()
        finally:
            await self.on_stop()
            if self.redis:
                await self.redis.aclose()
            await self.consumer.stop()
            await self.dlq_producer.stop()
