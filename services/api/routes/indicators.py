import asyncio
import json

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

router = APIRouter()


@router.websocket("/{product_id}")
async def stream_product_indicators(websocket: WebSocket, product_id: str):
    await websocket.accept()
    r = websocket.app.state.redis
    exchange = websocket.query_params.get("exchange", "coinbase")
    product_key = f"{exchange}:{product_id}"

    try:
        while True:
            keys = [key async for key in r.scan_iter(f"indicator:{product_key}:*")]

            indicators = {}
            for key in keys:
                value = await r.get(key)
                if value is not None:
                    name = key.decode().removeprefix(f"indicator:{product_key}:")
                    indicators[name] = float(value)

            await websocket.send_text(json.dumps(indicators))
            await asyncio.sleep(0.05)
    except WebSocketDisconnect:
        pass
