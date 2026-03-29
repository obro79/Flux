from .Base import BaseExchange
import websockets
import json
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
                            "channel": "ticker",
                        }
                    )
                )

                async for message in websocket:
                    raw = (
                        message.encode() if isinstance(message, str) else bytes(message)
                    )
                    await self.publish("market_trades", raw)
        finally:
            await self.stop_producer()
