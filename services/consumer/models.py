from pydantic import BaseModel


class Ticker(BaseModel):
    product_id: str
    price: float
    volume_24_h: float = 0.0


class Event(BaseModel):
    tickers: list[Ticker] = []


class MarketTradeMessage(BaseModel):
    events: list[Event] = []
