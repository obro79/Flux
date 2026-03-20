from __future__ import annotations

from services.ingestion.exchanges.base import BaseExchange


class KrakenExchange(BaseExchange):
    @property
    def name(self) -> str:
        return "kraken"

    async def run(self) -> None:
        raise NotImplementedError("Kraken WebSocket ingestion is not implemented yet.")
