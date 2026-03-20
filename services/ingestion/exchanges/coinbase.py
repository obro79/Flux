from __future__ import annotations

from services.ingestion.exchanges.base import BaseExchange


class CoinbaseExchange(BaseExchange):
    @property
    def name(self) -> str:
        return "coinbase"

    async def run(self) -> None:
        raise NotImplementedError(
            "Coinbase WebSocket ingestion is not implemented yet."
        )
