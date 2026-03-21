from .Base import BaseExchange
import websockets
import json


class CoinbaseExchange(BaseExchange):
    def __init__(self) -> None:
        super().__init__()
        self.websocket_url: str = "wss://advanced-trade-ws.coinbase.com"
        self.tickers: list[str] = ["BTC-USD"]

    @property
    def name(self) -> str:
        return "coinbase"

    async def run(self) -> None:
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
                data = json.loads(message)
                print(f"Received data: {data}")
