import asyncio
import sys
from pathlib import Path

# Add project root for "services.*" imports, and this dir for local imports
sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from indicator_engine_consumer import IndicatorEngineConsumer
from ticker_consumer import TickerConsumer
from Indicators import RunningSMA, RunningRSI


async def main():
    engine = IndicatorEngineConsumer()
    engine.add_indicator("sma_5", RunningSMA(window_size=5))
    engine.add_indicator("rsi_14", RunningRSI(window_size=14))

    ticker = TickerConsumer()

    await asyncio.gather(engine.run(), ticker.run())


if __name__ == "__main__":
    asyncio.run(main())
