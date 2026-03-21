import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from exchanges.Coinbase import CoinbaseExchange


def main():
    """Main function for the ingestion service."""
    print("Starting ingestion service...")
    coinbase_exchange = CoinbaseExchange()
    asyncio.run(coinbase_exchange.run())


if __name__ == "__main__":
    main()
