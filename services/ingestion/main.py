import asyncio
import sys
from pathlib import Path

from prometheus_client import start_http_server

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from exchanges.coinbase import CoinbaseExchange
from exchanges.kraken import KrakenExchange


async def main() -> None:
    """Main function for the ingestion service."""
    print("Starting ingestion service...")
    start_http_server(8001)
    exchanges = [CoinbaseExchange(), KrakenExchange()]
    await asyncio.gather(*(exchange.run() for exchange in exchanges))


if __name__ == "__main__":
    asyncio.run(main())
