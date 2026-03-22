from fastapi import APIRouter
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
    resolution: str


@router.get("/candle/{product_id}/{resolution}", response_model=Candle)
async def get_candles(product_id: str, resolution: str):
    pass
