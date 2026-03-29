from __future__ import annotations

from abc import ABC, abstractmethod

from aiokafka import AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic


class BaseExchange(ABC):
    def __init__(self, bootstrap_servers: str = "localhost:9092") -> None:
        self.websocket_url: str = ""
        self.producer: AIOKafkaProducer | None = None
        self.bootstrap_servers = bootstrap_servers

    @property
    @abstractmethod
    def name(self) -> str:
        """Return the exchange name."""
        return self.__class__.__name__.lower()

    async def ensure_topics(self, topics: list[str]) -> None:
        admin = AIOKafkaAdminClient(bootstrap_servers=self.bootstrap_servers)
        await admin.start()
        try:
            existing = await admin.list_topics()
            new_topics = [
                NewTopic(name=t, num_partitions=1, replication_factor=1)
                for t in topics
                if t not in existing
            ]
            if new_topics:
                await admin.create_topics(new_topics)
        finally:
            await admin.close()

    async def start_producer(self) -> None:
        await self.ensure_topics(["market_trades", "market_trades.dlq"])
        producer = AIOKafkaProducer(bootstrap_servers=self.bootstrap_servers)
        await producer.start()
        self.producer = producer

    async def stop_producer(self) -> None:
        if self.producer:
            await self.producer.stop()

    async def publish(self, topic: str, message: bytes) -> None:
        if self.producer:
            await self.producer.send(topic, message)

    @abstractmethod
    async def run(self) -> None:
        """Start consuming and publishing market data."""
