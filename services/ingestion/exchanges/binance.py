from __future__ import annotations

from services.ingestion.exchanges.base import BaseExchange


class BinanceExchange(BaseExchange):
    @property
    def name(self) -> str:
        return "binance"

    async def run(self) -> None:
        raise NotImplementedError("Binance WebSocket ingestion is not implemented yet.")
