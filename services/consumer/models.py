from datetime import datetime

from pydantic import BaseModel, Field


class Trade(BaseModel):
    trade_id: str
    product_id: str
    price: float
    size: float
    side: str
    time: datetime


class Event(BaseModel):
    trades: list[Trade] = Field(default_factory=list)


class MarketTradeMessage(BaseModel):
    channel: str
    timestamp: datetime
    events: list[Event] = Field(default_factory=list)
