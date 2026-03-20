from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from services.ingestion.producer import KafkaProducerWrapper


class BaseExchange(ABC):
    def __init__(self, producer: KafkaProducerWrapper) -> None:
        self.producer = producer

    @property
    @abstractmethod
    def name(self) -> str:
        """Return the exchange name."""

    @abstractmethod
    async def run(self) -> None:
        """Start consuming and publishing market data."""

    def publish(self, payload: dict[str, Any]) -> None:
        self.producer.send(payload, topic=self.name)
