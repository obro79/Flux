import asyncio
import logging
from datetime import datetime, timedelta, timezone

from base_consumer import BaseConsumer
from models import Trade
from services.database.database import Database
from utils import retry_policy

logger = logging.getLogger(__name__)


def minute_bucket(timestamp: datetime) -> datetime:
    return timestamp.astimezone(timezone.utc).replace(second=0, microsecond=0)


class CandleBuffer:
    def __init__(self) -> None:
        self.open: float | None = None
        self.high: float = float("-inf")
        self.low: float = float("inf")
        self.close: float = 0.0
        self.volume: float = 0.0
        self.open_time: datetime | None = None
        self.close_time: datetime | None = None

    def add_trade(self, trade_time: datetime, price: float, size: float) -> None:
        if self.open is None:
            self.open = price
            self.open_time = trade_time
        elif self.open_time is not None and trade_time < self.open_time:
            self.open = price
            self.open_time = trade_time

        if self.close_time is None or trade_time >= self.close_time:
            self.close = price
            self.close_time = trade_time

        self.high = max(self.high, price)
        self.low = min(self.low, price)
        self.volume += size

    def is_empty(self) -> bool:
        return self.open is None

    def to_dict(self) -> dict[str, float | None]:
        return {
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
        }


class CandleAggregator:
    def __init__(self, grace_period: timedelta = timedelta(seconds=5)) -> None:
        self.buffers: dict[tuple[str, str], dict[datetime, CandleBuffer]] = {}
        self.grace_period = grace_period

    def add_trade(self, trade: Trade) -> None:
        product_buffers = self.buffers.setdefault(
            (trade.exchange, trade.product_id),
            {},
        )
        bucket = minute_bucket(trade.time)
        buffer = product_buffers.setdefault(bucket, CandleBuffer())
        buffer.add_trade(trade.time, trade.price, trade.size)

    def flush_ready(self, now: datetime | None = None) -> list[dict[str, object]]:
        reference_time = now or datetime.now(timezone.utc)
        ready: list[dict[str, object]] = []

        for (exchange, product_id), product_buffers in list(self.buffers.items()):
            ready_minutes = sorted(
                bucket
                for bucket in product_buffers
                if reference_time >= bucket + timedelta(minutes=1) + self.grace_period
            )
            for bucket in ready_minutes:
                ready.append(
                    {
                        "exchange": exchange,
                        "product_id": product_id,
                        "timestamp": bucket,
                        **product_buffers.pop(bucket).to_dict(),
                    }
                )
            if not product_buffers:
                del self.buffers[(exchange, product_id)]

        return ready

    def flush_all(self) -> list[dict[str, object]]:
        ready: list[dict[str, object]] = []

        for (exchange, product_id), product_buffers in list(self.buffers.items()):
            for bucket in sorted(product_buffers):
                ready.append(
                    {
                        "exchange": exchange,
                        "product_id": product_id,
                        "timestamp": bucket,
                        **product_buffers[bucket].to_dict(),
                    }
                )
            del self.buffers[(exchange, product_id)]

        return ready


class TickerConsumer(BaseConsumer):
    def __init__(
        self,
        db: Database | None = None,
        flush_grace_period: timedelta = timedelta(seconds=5),
    ) -> None:
        super().__init__(group_id="candle_builder")
        self.aggregator = CandleAggregator(grace_period=flush_grace_period)
        self.db = db or Database()
        self._flush_task: asyncio.Task | None = None

    @retry_policy
    def flush_ready_candles(self, now: datetime | None = None) -> None:
        for candle in self.aggregator.flush_ready(now=now):
            self.db.insert_candle(candle)
            logger.info(
                "Flushed candle",
                extra={
                    "exchange": candle["exchange"],
                    "product_id": candle["product_id"],
                    "timestamp": candle["timestamp"],
                    "candle": candle,
                },
            )

    @retry_policy
    def flush_all_candles(self) -> None:
        for candle in self.aggregator.flush_all():
            self.db.insert_candle(candle)
            logger.info(
                "Flushed final candle",
                extra={
                    "exchange": candle["exchange"],
                    "product_id": candle["product_id"],
                    "timestamp": candle["timestamp"],
                    "candle": candle,
                },
            )

    async def flush_loop(self) -> None:
        while True:
            await asyncio.sleep(1)
            try:
                self.flush_ready_candles()
            except Exception:
                logger.exception("Flush error")

    async def process_trade(self, trade: Trade) -> None:
        self.aggregator.add_trade(trade)

    async def on_start(self) -> None:
        self._flush_task = asyncio.create_task(self.flush_loop())

    async def on_stop(self) -> None:
        if self._flush_task:
            self._flush_task.cancel()
        self.flush_all_candles()
        self.db.disconnect()


if __name__ == "__main__":
    consumer = TickerConsumer()
    asyncio.run(consumer.run())
