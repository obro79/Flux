import asyncio
import json

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

router = APIRouter()


@router.websocket("/{product_id}")
async def stream_product_data(websocket: WebSocket, product_id: str):
    await websocket.accept()
    r = websocket.app.state.redis
    exchange = websocket.query_params.get("exchange", "coinbase")
    product_key = f"{exchange}:{product_id}"

    try:
        while True:
            data = {}

            # Raw price data
            for key in [k async for k in r.scan_iter(f"crypto:{product_key}:*")]:
                value = await r.get(key)
                if value is not None:
                    name = key.decode().removeprefix(f"crypto:{product_key}:")
                    data[name] = float(value)

            # Indicators
            for key in [k async for k in r.scan_iter(f"indicator:{product_key}:*")]:
                value = await r.get(key)
                if value is not None:
                    name = key.decode().removeprefix(f"indicator:{product_key}:")
                    data[name] = float(value)

            await websocket.send_text(json.dumps(data))
            await asyncio.sleep(0.05)
    except WebSocketDisconnect:
        pass
