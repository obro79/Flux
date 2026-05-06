import asyncio
from contextlib import suppress
import sys
from pathlib import Path
# Add project root for "services.*" imports, this dir for local imports, and services/ for utils
sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from indicator_consumer import IndicatorEngineConsumer
from ticker_consumer import TickerConsumer
from raw_consumer import RawConsumer
from indicators import RunningEMA, RunningSMA, RunningRSI
from prometheus_client import start_http_server


async def main():
    engine = IndicatorEngineConsumer()
    engine.add_indicator("sma_5", RunningSMA, window_size=5)
    engine.add_indicator("rsi_14", RunningRSI, window_size=14)
    engine.add_indicator("ema_12", RunningEMA, window_size=12)

    ticker = TickerConsumer()
    raw = RawConsumer()
    start_http_server(8002)  # Start Prometheus metrics server for consumer
    tasks = [
        asyncio.create_task(engine.run(), name="indicator-engine"),
        asyncio.create_task(ticker.run(), name="ticker-consumer"),
        asyncio.create_task(raw.run(), name="raw-consumer"),
    ]
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        for task in tasks:
            task.cancel()
        for task in tasks:
            with suppress(asyncio.CancelledError):
                await task
        raise


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
