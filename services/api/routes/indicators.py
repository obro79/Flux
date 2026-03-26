import asyncio
import json

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

router = APIRouter()


@router.websocket("/indicators")
async def stream_indicators(websocket: WebSocket):
    await websocket.accept()
    r = websocket.app.state.redis

    try:
        while True:
            keys = [key async for key in r.scan_iter("indicator:*")]

            indicators = {}
            for key in keys:
                value = await r.get(key)
                if value is not None:
                    name = key.decode().removeprefix("indicator:")
                    indicators[name] = float(value)

            await websocket.send_text(json.dumps(indicators))
            await asyncio.sleep(0.01)
    except WebSocketDisconnect:
        pass
