from __future__ import annotations

from abc import ABC, abstractmethod
from aiokafka import AIOKafkaProducer


class BaseExchange(ABC):
    def __init__(self, bootstrap_servers: str = "localhost:9092") -> None:
        self.websocket_url: str = ""
        self.producer: AIOKafkaProducer | None = None
        self.bootstrap_servers = bootstrap_servers

    @property
    @abstractmethod
    def name(self) -> str:
        """Return the exchange name."""

    async def start_producer(self) -> None:
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
