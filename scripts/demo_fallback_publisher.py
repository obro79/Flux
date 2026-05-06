import asyncio
import math
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from aiokafka import AIOKafkaProducer

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from services.config import csv_env, kafka_bootstrap_servers
from services.market_data.models import MarketTradeMessage, Trade


BASE_PRICES = {
    "BTC-USD": 100000.0,
    "ETH-USD": 3500.0,
    "SOL-USD": 150.0,
}


def enabled() -> bool:
    return os.getenv("DEMO_FALLBACK_ENABLED", "true").lower() in {"1", "true", "yes"}


def build_demo_trade(product_id: str, sequence: int) -> bytes:
    timestamp = datetime.now(timezone.utc)
    base_price = BASE_PRICES.get(product_id, 100.0)
    wave = math.sin(sequence / 10) * (base_price * 0.002)
    price = round(base_price + wave + (sequence % 7), 2)
    trade = Trade(
        exchange="demo",
        trade_id=f"demo-{product_id}-{int(time.time())}-{sequence}",
        product_id=product_id,
        price=price,
        size=round(0.01 + ((sequence % 5) * 0.002), 6),
        side="BUY" if sequence % 2 == 0 else "SELL",
        time=timestamp,
    )
    return (
        MarketTradeMessage.from_trades(
            exchange="demo",
            trades=[trade],
            timestamp=timestamp,
        )
        .model_dump_json()
        .encode()
    )


async def main() -> None:
    if not enabled():
        return

    products = csv_env("DEMO_PRODUCT_IDS", "BTC-USD,ETH-USD,SOL-USD")
    producer = AIOKafkaProducer(bootstrap_servers=kafka_bootstrap_servers())
    await producer.start()
    sequence = 0
    try:
        while True:
            for product_id in products:
                await producer.send_and_wait(
                    "market_trades",
                    build_demo_trade(product_id, sequence),
                )
            sequence += 1
            await asyncio.sleep(float(os.getenv("DEMO_FALLBACK_INTERVAL_SECONDS", "5")))
    finally:
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
