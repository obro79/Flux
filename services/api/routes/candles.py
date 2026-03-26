from fastapi import APIRouter, Query, Request
from pydantic import BaseModel
from datetime import datetime

router = APIRouter()


class Candle(BaseModel):
    product_id: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float


@router.get("/{product_id}/{resolution}", response_model=list[Candle])
async def get_candles(
    request: Request,
    product_id: str,
    resolution: str,
    limit: int = Query(default=100, le=1000),
):
    if resolution != "1m":
        return []

    db = request.app.state.db
    rows = db.get_candles(product_id, limit)
    return rows
