import asyncio
import json
import websockets

COINBASE_WS_URL = "wss://advanced-trade-ws.coinbase.com"


async def subscribe_to_coinbase():
    async with websockets.connect(COINBASE_WS_URL) as websocket:
        await websocket.send(
            json.dumps(
                {
                    "type": "subscribe",
                    "product_ids": ["BTC-USD"],
                    "channel": "ticker",
                }
            )
        )

        async for message in websocket:
            data = json.loads(message)
            print(f"Received data: {data}")


if __name__ == "__main__":
    asyncio.run(subscribe_to_coinbase())
