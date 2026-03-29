import json
import asyncio
from datetime import datetime, timezone
import aiokafka
from services.consumer.models import MarketTradeMessage
from services.database.database import Database
from .dead_letter_queue import publish_to_dlq
from utils import retry_policy


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


class TickerConsumer:
    def __init__(self) -> None:
        self.buffers: dict[str, CandleBuffer] = {}
        self.db = Database()
        self.consumer = aiokafka.AIOKafkaConsumer(
            "market_trades",
            bootstrap_servers="localhost:9092",
            group_id="candle_builder",
            value_deserializer=lambda x: json.loads(x) if x else None,
            auto_offset_reset="earliest",
        )
        self.dlq_producer = aiokafka.AIOKafkaProducer(
            bootstrap_servers="localhost:9092"
        )

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
                print(f"Flushed candle: {product_id} | {candle}")
                buffer.reset()

    async def flush_loop(self) -> None:
        while True:
            await asyncio.sleep(60)
            print(f"Flushing candles... ({len(self.buffers)} buffers)")
            try:
                self.flush_candles()
                print("Flush complete")
            except Exception as e:
                print(f"Flush error: {e}")

    async def run(self) -> None:
        await self.consumer.start()
        await self.dlq_producer.start()
        flush_task = asyncio.create_task(self.flush_loop())
        try:
            async for message in self.consumer:
                try:
                    if message.value is None:
                        continue
                    msg = MarketTradeMessage(**message.value)
                    for event in msg.events:
                        for ticker in event.tickers:
                            self.get_buffer(ticker.product_id).add_trade(ticker.price, ticker.volume_24_h)
                except Exception as e:
                    await publish_to_dlq(self.dlq_producer, message, e)
        finally:
            flush_task.cancel()
            self.flush_candles()
            await self.consumer.stop()
            await self.dlq_producer.stop()
            self.db.disconnect()


if __name__ == "__main__":
    consumer = TickerConsumer()
    asyncio.run(consumer.run())
