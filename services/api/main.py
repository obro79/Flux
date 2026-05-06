import os
import time
from collections import defaultdict, deque
from fastapi import FastAPI
from starlette.requests import Request
from starlette.responses import Response
from prometheus_fastapi_instrumentator import Instrumentator
from contextlib import asynccontextmanager
import redis.asyncio as redis
from services.database.database import Database
from services.api.routes.candles import router as candles_router
from services.api.routes.crypto import router as crypto_router
from services.api.routes.health import router as health_router
from services.api.routes.indicators import router as indicators_router
from services.api.routes.markets import router as markets_router


RATE_LIMIT_PER_MINUTE = int(os.getenv("RATE_LIMIT_PER_MINUTE", "120"))
RATE_LIMIT_PATH_PREFIXES = ("/candles", "/crypto", "/indicators", "/markets", "/docs", "/openapi.json")
request_times: dict[str, deque[float]] = defaultdict(deque)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.redis = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379"))
    app.state.db = Database()

    yield

    await app.state.redis.aclose()
    app.state.db.disconnect()


app: FastAPI = FastAPI(lifespan=lifespan)


@app.middleware("http")
async def rate_limit_public_api(request: Request, call_next):
    if RATE_LIMIT_PER_MINUTE > 0 and request.url.path.startswith(RATE_LIMIT_PATH_PREFIXES):
        client = request.client.host if request.client else "unknown"
        now = time.monotonic()
        window_start = now - 60
        times = request_times[client]
        while times and times[0] < window_start:
            times.popleft()
        if len(times) >= RATE_LIMIT_PER_MINUTE:
            return Response("rate limit exceeded", status_code=429)
        times.append(now)
    return await call_next(request)


Instrumentator().instrument(app).expose(app)

app.include_router(health_router)
app.include_router(markets_router)
app.include_router(candles_router, prefix="/candles")
app.include_router(crypto_router, prefix="/crypto")
app.include_router(indicators_router, prefix="/indicators")
