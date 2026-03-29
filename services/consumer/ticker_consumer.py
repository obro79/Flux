import asyncio
import logging
from datetime import datetime, timezone

from base_consumer import BaseConsumer
from models import Ticker
from services.database.database import Database
from utils import retry_policy

logger = logging.getLogger(__name__)


class CandleBuffer:
    def __init__(self) -> None:
        self.open: float | None = None
        self.high: float = float("-inf")
        self.low: float = float("inf")
        self.close: float = 0.0
        self.volume: float = 0.0

    def add_trade(self, price: float, size: float):
        if self.open is None:
            self.open = price
        self.high = max(self.high, price)
        self.low = min(self.low, price)
        self.close = price
        self.volume += size

    def is_empty(self) -> bool:
        return self.open is None

    def to_dict(self) -> dict:
        return {
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
        }

    def reset(self):
        self.open = None
        self.high = float("-inf")
        self.low = float("inf")
        self.close = 0.0
        self.volume = 0.0


class TickerConsumer(BaseConsumer):
    def __init__(self) -> None:
        super().__init__(group_id="candle_builder")
        self.buffers: dict[str, CandleBuffer] = {}
        self.db = Database()
        self._flush_task: asyncio.Task | None = None

    def get_buffer(self, product_id: str) -> CandleBuffer:
        if product_id not in self.buffers:
            self.buffers[product_id] = CandleBuffer()
        return self.buffers[product_id]

    @retry_policy
    def flush_candles(self) -> None:
        timestamp = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        for product_id, buffer in self.buffers.items():
            if not buffer.is_empty():
                candle = buffer.to_dict()
                candle["product_id"] = product_id
                candle["timestamp"] = timestamp
                self.db.insert_candle(candle)
                logger.info(
                    "Flushed candle", extra={"product_id": product_id, "candle": candle}
                )
                buffer.reset()

    async def flush_loop(self) -> None:
        while True:
            await asyncio.sleep(60)
            logger.info("Flushing candles", extra={"buffer_count": len(self.buffers)})
            try:
                self.flush_candles()
                logger.info("Flush complete")
            except Exception:
                logger.exception("Flush error")

    async def process_ticker(self, ticker: Ticker) -> None:
        self.get_buffer(ticker.product_id).add_trade(
            ticker.price, ticker.volume_24_h
        )

    async def on_start(self) -> None:
        self._flush_task = asyncio.create_task(self.flush_loop())

    async def on_stop(self) -> None:
        if self._flush_task:
            self._flush_task.cancel()
        self.flush_candles()
        self.db.disconnect()


if __name__ == "__main__":
    consumer = TickerConsumer()
    asyncio.run(consumer.run())
