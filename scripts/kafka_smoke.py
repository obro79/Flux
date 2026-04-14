import argparse
import asyncio
import sys
from datetime import datetime, timezone
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
    parser.add_argument("--malformed", action="store_true")
    return parser


async def main() -> None:
    args = build_parser().parse_args()
    producer = AIOKafkaProducer(bootstrap_servers=args.bootstrap_server)
    await producer.start()
    try:
        for index in range(args.count):
            if args.malformed:
                payload = b'{"exchange":"unknown","broken":true'
            else:
                now = datetime.now(timezone.utc)
                trade = Trade(
                    exchange=args.exchange,
                    trade_id=f"smoke-{index}",
                    product_id=args.product_id,
                    price=100000 + index,
                    size=0.01,
                    side="BUY",
                    time=now,
                )
                payload = (
                    MarketTradeMessage.from_trades(
                        exchange=args.exchange,
                        trades=[trade],
                        timestamp=now,
                    )
                    .model_dump_json()
                    .encode()
                )

            await producer.send_and_wait(args.topic, payload)
            if args.delay_ms:
                await asyncio.sleep(args.delay_ms / 1000)
    finally:
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
