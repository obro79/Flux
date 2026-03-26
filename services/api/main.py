import os
from fastapi import FastAPI
from contextlib import asynccontextmanager
import redis.asyncio as redis
from services.database.database import Database
from services.api.routes.candles import router as candles_router
from services.api.routes.indicators import router as indicators_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.redis = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379"))
    app.state.db = Database()

    yield

    await app.state.redis.aclose()
    app.state.db.disconnect()


app: FastAPI = FastAPI(lifespan=lifespan)


app.include_router(candles_router)
app.include_router(indicators_router)
