from .base import BaseExchange
import json
import websockets

from exchanges.normalizers import normalize_coinbase_message
from utils import retry_policy


class CoinbaseExchange(BaseExchange):
    def __init__(self) -> None:
        super().__init__()
        self.websocket_url: str = "wss://advanced-trade-ws.coinbase.com"
        self.tickers: list[str] = ["BTC-USD", "ETH-USD", "SOL-USD"]

    @property
    def name(self) -> str:
        return "coinbase"

    @retry_policy
    async def run(self) -> None:
        await self.start_producer()
        try:
            async with websockets.connect(self.websocket_url) as websocket:
                await websocket.send(
                    json.dumps(
                        {
                            "type": "subscribe",
                            "product_ids": [ticker for ticker in self.tickers],
                            "channel": "market_trades",
                        }
                    )
                )

                async for message in websocket:
                    payload = json.loads(message)
                    normalized = normalize_coinbase_message(payload)
                    if normalized is None:
                        continue
                    await self.publish(
                        "market_trades", normalized.model_dump_json().encode()
                    )
        finally:
            await self.stop_producer()
