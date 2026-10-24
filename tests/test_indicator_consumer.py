import asyncio
from datetime import UTC, datetime

from indicator_consumer import IndicatorEngineConsumer
from indicators import RunningRSI, RunningSMA
from models import Trade


class FakePipeline:
    def __init__(self, store: dict[str, float]) -> None:
        self._store = store
        self._pending: dict[str, float] = {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc) -> None:
        return None

    def set(self, key: str, value: float) -> None:
        self._pending[key] = value

    async def execute(self) -> None:
        self._store.update(self._pending)
        self._pending = {}


class FakeRedis:
    def __init__(self) -> None:
        self.store: dict[str, float] = {}

    def pipeline(self) -> FakePipeline:
        return FakePipeline(self.store)


def make_engine() -> IndicatorEngineConsumer:
    engine = IndicatorEngineConsumer.__new__(IndicatorEngineConsumer)
    engine.group_id = "indicator_engine_test"
    engine.redis = FakeRedis()
    engine.indicator_templates = {}
    engine.indicator_kwargs = {}
    engine.product_indicators = {}
    engine.add_indicator("sma_2", RunningSMA, window_size=2)
    engine.add_indicator("rsi_2", RunningRSI, window_size=2)
    return engine


def trade(exchange: str, price: float) -> Trade:
    return Trade(
        exchange=exchange,
        trade_id=f"{exchange}-{price}",
        product_id="BTC-USD",
        price=price,
        size=1.0,
        side="BUY",
        time=datetime(2026, 4, 13, 22, 40, tzinfo=UTC),
    )


def test_indicators_are_written_per_exchange_and_product() -> None:
    engine = make_engine()

    asyncio.run(engine.process_trade(trade("coinbase", 100.0)))
    asyncio.run(engine.process_trade(trade("kraken", 200.0)))
    asyncio.run(engine.process_trade(trade("coinbase", 102.0)))

    store = engine.redis.store
    assert store["indicator:coinbase:BTC-USD:sma_2"] == 101.0
    assert store["indicator:kraken:BTC-USD:sma_2"] == 200.0


def test_warming_indicators_are_not_written() -> None:
    engine = make_engine()

    asyncio.run(engine.process_trade(trade("coinbase", 100.0)))

    assert "indicator:coinbase:BTC-USD:rsi_2" not in engine.redis.store


def test_each_product_gets_independent_indicator_state() -> None:
    engine = make_engine()

    coinbase = engine.get_indicators("coinbase:BTC-USD")
    kraken = engine.get_indicators("kraken:BTC-USD")

    assert coinbase["sma_2"] is not kraken["sma_2"]
    assert engine.get_indicators("coinbase:BTC-USD") is coinbase
