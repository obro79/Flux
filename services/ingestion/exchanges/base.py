from __future__ import annotations

from abc import ABC, abstractmethod


class BaseExchange(ABC):
    def __init__(self) -> None:
        self.websocket_url: str = ""

    @property
    @abstractmethod
    def name(self) -> str:
        """Return the exchange name."""

    @abstractmethod
    async def run(self) -> None:
        """Start consuming and publishing market data."""
