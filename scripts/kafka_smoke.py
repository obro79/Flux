import argparse
import asyncio
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from aiokafka import AIOKafkaProducer

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from services.market_data.models import MarketTradeMessage, Trade


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Emit Kafka smoke traffic locally.")
    parser.add_argument("--bootstrap-server", default="localhost:9092")
    parser.add_argument("--topic", default="market_trades")
    parser.add_argument("--exchange", default="coinbase")
    parser.add_argument("--product-id", default="BTC-USD")
    parser.add_argument("--count", type=int, default=1)
    parser.add_argument("--delay-ms", type=int, default=0)
    parser.add_argument(
        "--age-seconds",
        type=float,
        default=0,
        help="Backdate generated trade timestamps by N seconds.",
    )
    parser.add_argument("--malformed", action="store_true")
    return parser


def build_trade_payload(
    *,
    exchange: str,
    product_id: str,
    index: int,
    age_seconds: float = 0,
    now: datetime | None = None,
) -> bytes:
    timestamp = now or datetime.now(timezone.utc)
    trade_time = timestamp - timedelta(seconds=age_seconds)
    trade = Trade(
        exchange=exchange,
        trade_id=f"smoke-{index}",
        product_id=product_id,
        price=100000 + index,
        size=0.01,
        side="BUY",
        time=trade_time,
    )
    return (
        MarketTradeMessage.from_trades(
            exchange=exchange,
            trades=[trade],
            timestamp=trade_time,
        )
        .model_dump_json()
        .encode()
    )


async def main() -> None:
    args = build_parser().parse_args()
    producer = AIOKafkaProducer(bootstrap_servers=args.bootstrap_server)
    await producer.start()
    try:
        for index in range(args.count):
            if args.malformed:
                payload = b'{"exchange":"unknown","broken":true'
            else:
                payload = build_trade_payload(
                    exchange=args.exchange,
                    product_id=args.product_id,
                    index=index,
                    age_seconds=args.age_seconds,
                )

            await producer.send_and_wait(args.topic, payload)
            if args.delay_ms:
                await asyncio.sleep(args.delay_ms / 1000)
    finally:
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
