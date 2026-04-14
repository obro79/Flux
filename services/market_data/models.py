from __future__ import annotations

from datetime import datetime, timezone

from pydantic import BaseModel, Field


class Trade(BaseModel):
    exchange: str
    trade_id: str
    product_id: str
    price: float
    size: float
    side: str
    time: datetime


class Event(BaseModel):
    type: str = "update"
    trades: list[Trade] = Field(default_factory=list)


class MarketTradeMessage(BaseModel):
    channel: str = "market_trades"
    exchange: str
    timestamp: datetime
    events: list[Event] = Field(default_factory=list)

    @classmethod
    def from_trades(
        cls,
        *,
        exchange: str,
        trades: list[Trade],
        event_type: str = "update",
        timestamp: datetime | None = None,
    ) -> "MarketTradeMessage":
        event_timestamp = timestamp or (
            max((trade.time for trade in trades), default=datetime.now(timezone.utc))
        )
        return cls(
            exchange=exchange,
            timestamp=event_timestamp,
            events=[Event(type=event_type, trades=trades)],
        )
