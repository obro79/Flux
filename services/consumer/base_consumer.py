import asyncio
import json
import logging
from abc import ABC, abstractmethod
from contextlib import suppress

import aiokafka
import redis.asyncio as aioredis

from dead_letter_queue import publish_to_dlq
from models import MarketTradeMessage, Trade
from metrics import consumer_lag, dlq_messages_total, messages_consumed_total

logger = logging.getLogger(__name__)


class BaseConsumer(ABC):
    def __init__(
        self,
        group_id: str,
        redis_url: str | None = None,
        bootstrap_servers: str = "localhost:9092",
    ) -> None:
        self.group_id = group_id
        self.topic = "market_trades"
        self.consumer = aiokafka.AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            value_deserializer=lambda x: json.loads(x) if x else None,
            auto_offset_reset="earliest",
        )
        self.dlq_producer = aiokafka.AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers
        )
        self.redis = aioredis.from_url(redis_url) if redis_url else None
        self._lag_task: asyncio.Task | None = None

    @abstractmethod
    async def process_trade(self, trade: Trade) -> None:
        """Handle a single trade update."""

    async def on_start(self) -> None:
        """Hook for subclasses to run setup after Kafka connects."""

    async def on_stop(self) -> None:
        """Hook for subclasses to run cleanup before shutdown."""

    async def lag_loop(self) -> None:
        while True:
            await self.update_lag_metrics()
            await asyncio.sleep(5)

    async def update_lag_metrics(self) -> None:
        assignments = self.consumer.assignment()
        if not assignments:
            return

        end_offsets = await self.consumer.end_offsets(assignments)
        for partition in assignments:
            position = await self.consumer.position(partition)
            lag = max(end_offsets.get(partition, position) - position, 0)
            consumer_lag.labels(
                consumer_group=self.group_id,
                topic=partition.topic,
                partition=str(partition.partition),
            ).set(lag)

    @staticmethod
    def extract_exchange(payload: dict | None) -> str:
        if not payload:
            return "unknown"
        exchange = payload.get("exchange")
        if isinstance(exchange, str) and exchange:
            return exchange
        return "unknown"

    async def run(self) -> None:
        await self.consumer.start()
        await self.dlq_producer.start()
        self._lag_task = asyncio.create_task(self.lag_loop())
        await self.on_start()
        try:
            async for message in self.consumer:
                try:
                    if message.value is None:
                        continue
                    msg = MarketTradeMessage(**message.value)
                    messages_consumed_total.labels(
                        consumer_group=self.group_id,
                        topic=message.topic,
                        exchange=msg.exchange,
                    ).inc()
                    for event in msg.events:
                        for trade in event.trades:
                            await self.process_trade(trade)
                except Exception as exc:
                    exchange = self.extract_exchange(message.value)
                    await publish_to_dlq(
                        self.dlq_producer,
                        message,
                        exc,
                        exchange=exchange,
                    )
                    dlq_messages_total.labels(
                        consumer_group=self.group_id,
                        topic=message.topic,
                        error_type=type(exc).__name__,
                        exchange=exchange,
                    ).inc()
        finally:
            if self._lag_task:
                self._lag_task.cancel()
                with suppress(asyncio.CancelledError):
                    await self._lag_task
            await self.on_stop()
            if self.redis:
                await self.redis.aclose()
            await self.consumer.stop()
            await self.dlq_producer.stop()
