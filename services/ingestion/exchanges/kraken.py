from __future__ import annotations

import json

import websockets

from .base import BaseExchange
from exchanges.normalizers import normalize_kraken_message
from utils import retry_policy


class KrakenExchange(BaseExchange):
    def __init__(self) -> None:
        super().__init__()
        self.websocket_url = "wss://ws.kraken.com/v2"
        self.tickers: list[str] = ["BTC-USD", "ETH-USD", "SOL-USD"]
        self.symbol_map = {
            "BTC/USD": "BTC-USD",
            "ETH/USD": "ETH-USD",
            "SOL/USD": "SOL-USD",
        }

    @property
    def name(self) -> str:
        return "kraken"

    @retry_policy
    async def run(self) -> None:
        await self.start_producer()
        try:
            async with websockets.connect(self.websocket_url) as websocket:
                await websocket.send(
                    json.dumps(
                        {
                            "method": "subscribe",
                            "params": {
                                "channel": "trade",
                                "snapshot": False,
                                "symbol": list(self.symbol_map.keys()),
                            },
                        }
                    )
                )

                async for message in websocket:
                    payload = json.loads(message)
                    normalized = normalize_kraken_message(
                        payload,
                        symbol_map=self.symbol_map,
                    )
                    if normalized is None:
                        continue
                    await self.publish(
                        "market_trades", normalized.model_dump_json().encode()
                    )
        finally:
            await self.stop_producer()
