import os
from fastapi import FastAPI
from contextlib import asynccontextmanager
import redis.asyncio as redis
from services.database.database import Database


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.redis = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379"))
    app.state.db = Database()

    yield

    await app.state.redis.aclose()
    app.state.db.disconnect()


app: FastAPI = FastAPI(lifespan=lifespan)
